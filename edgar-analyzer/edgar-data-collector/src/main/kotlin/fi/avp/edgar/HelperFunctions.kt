package fi.avp.edgar

import com.mongodb.BasicDBObject
import fi.avp.util.forEachAsync
import fi.avp.util.mapAsync
import fi.avp.util.runOnComputationThreadPool
import kotlinx.coroutines.*
import kotlinx.serialization.Serializable
import org.bson.Document
import org.bson.types.ObjectId
import org.litote.kmongo.*
import org.litote.kmongo.coroutine.*
import java.nio.file.Paths
import java.time.LocalDate
import java.util.concurrent.Executors
import kotlin.system.measureTimeMillis
import kotlin.time.measureTimedValue


@Serializable
data class CompanyInfo(
    val _id: String? = null,
    val primaryTicker: String,
    val tickers: Set<String> = emptySet(),
    val isInSP500: Boolean = false,
    val cik: Set<Int>,
    val names: Set<String>,
    val description: String = "",
    val exchange: String = "",
    val sic: Int = -1)

@Serializable
data class SecCompanyRecord(
    val cik_str: Int,
    val title: String,
    val ticker: String)

/**
 * Take the data from sec (json file extract) and company info from internet.
 * Sec data is considered as primary (has more actual ticker and sic numbers).
 * The algorithms collects all the records that have the same sic number or the same
 * ticker and aggregates them into a single record.
 */
suspend fun consolidateCompanyInfo() {
    val secTickerDatabase = Database.database.getCollection<SecCompanyRecord>("sec-ticker-database")
        .find()
        .toList()

    val groupedSecRecords = HashMap<SecCompanyRecord, MutableSet<SecCompanyRecord>>()
    secTickerDatabase.map {secRecord ->
        val existingRecord = groupedSecRecords
            .entries
            .find { it.key.ticker == secRecord.ticker || it.key.cik_str == secRecord.cik_str || it.key.title == secRecord.title }

        if (existingRecord == null) {
            groupedSecRecords[secRecord] = hashSetOf(secRecord)
        } else {
            existingRecord.value.add(secRecord)
        }
    }

    val companyInfoFromSec = groupedSecRecords.mapValues {
        val tickers = it.value.map { it.ticker }.toSet()
        val cik = it.value.map { it.cik_str }.toSet()
        val titles = it.value.map { it.title }.toSet()

        CompanyInfo(
            primaryTicker = tickers.minBy { it.length }!!,
            tickers = tickers,
            cik = cik,
            names = titles
        )

    }.values.toMutableList()

    val companyInfoDatabase = Database.database.getCollection<Document>("company-info").find().toList()

    val withAdditionalInformation = companyInfoDatabase.map { info ->
        val ticker = info.get("ticker").toString()
        val cik = info.getInteger("cik") ?: -1
        val infoFromSec = companyInfoFromSec.find { it.cik.contains(cik) || it.tickers.contains(ticker) }
        val sic = info["SIC"].let {
            when (it) {
                is Int -> it
                is Double -> it.toInt()
                "" -> -1
                is String -> it.toInt()
                else -> -1
            }
        }
        val exchange = info.getString("Exchange")
        val name = info["Name"].toString()

        infoFromSec?.let {
            it.copy(
                exchange = exchange,
                tickers = it.tickers.plus(ticker),
                cik = it.cik.plus(cik),
                names = it.names.plus(name),
                sic = sic)
        } ?: CompanyInfo(
            primaryTicker = ticker,
            exchange = exchange,
            cik = setOf(cik),
            names = setOf(name),
            tickers = setOf(ticker),
            sic = sic
        )
    }

    val companyList = Database.database.getCollection<CompanyInfo>("company-list")
    companyList.drop()

    companyInfoFromSec
        .filter { infoFromSec -> withAdditionalInformation.none { it.primaryTicker == infoFromSec.primaryTicker } }
        .plus(withAdditionalInformation)
        .forEach {
            companyList.insertOne(it)
        }

//    val sP500Tickers = Database.getSP500Tickers()
    companyList.find().toList().forEach {
        var tickers = it.tickers
            .flatMap { setOf(it, it.replace('-', '.'), it.replace('.', '-')) }.toSet()

        if (it.tickers.contains("GOOG")) {
           tickers = tickers.plus("GOOGL")
        }

//        if (it.tickers.any { sP500Tickers.contains(it) }) {
//            companyList.replaceOne(it.copy(isInSP500 = true))
//        }
        companyList.replaceOne(it.copy(tickers = tickers))
    }
}

suspend fun extractMetricsFromAllFilings() {

}

suspend fun findReportsWithMultipleEPS() {
    val reports = Database.database.getCollection<Filing>("sp500")
    val allReports = reports
        .find()
        .batchSize(10000).toList()

    allReports.filter {
        (it.extractedData?.filter {
            it.propertyId == "EarningsPerShareDiluted"
        }?.size ?: 0) > 1
    }.forEach {
        println(it.dataUrl)
    }
}

fun main() {
//    dump10KReportsToCSVRowPerCompany()
//    dump10KReportsToCSVRowPerFiling()
//    fixDecimalsInSP500AnnualReports()
//    consolidateCompanyInfo()
//    resolveAnnualReportReferencesSince()
    runBlocking {
//        fixDataExtractionStatus()
        resolveAnnualReferencesAndYearToYearDiffs(1)
//        dump10KReportsToCSVRowPerFiling()
    }
}

val years = 2011..2019
val metrics = mapOf(
    "revenue" to Filing::revenue,
    "netIncome" to Filing::netIncome,
    "eps" to Filing::eps,
    "assets" to Filing::revenue,
    "financingCashFlow" to Filing::financingCashFlow,
    "investingCashFlow" to Filing::investingCashFlow,
    "operatingCashFlow" to Filing::operatingCashFlow,
    "liabilities" to Filing::liabilities)

suspend fun extractMetrics(update: (Filing) -> Filing) {
    val reports = Database.database.getCollection<Filing>("sp500")
    val allReports = reports
        .find("{formType: '10-K'}")
        .batchSize(10000).toList()

    println("analyzing ${allReports.size} reports")
    runBlocking {
        withContext(Executors.newFixedThreadPool(16).asCoroutineDispatcher()) {
            allReports.chunked(100).mapAsync {
                it.forEach {
                    reports.updateOne(update(it))
                }
            }
        }
        println("done")
    }
}

suspend fun getClosestAnnualReportId(filing: Filing): ObjectId? {
    val date = filing.dateFiled!!
    val sortCriteria = BasicDBObject("dateFiled", -1)
    return Database.filings
        .find(and(Filing::dateFiled lt date, Filing::formType eq "10-K"))
        .sort(sortCriteria)
        .first()
        ?._id
}

fun resolveAnnualReportReferencesSince() {
    val ms = measureTimeMillis {
        val startDate = LocalDate.of(2017 , 12, 31)
        runBlocking {
            (0..3)
                .map { startDate.plusDays((365 * it).toLong()) to startDate.plusDays((365 * (it + 1)).toLong()) }
                .mapAsync {
                    println("looking between ${it.first} and ${it.second}")
                    Database.filings.find(and(
                        Filing::dateFiled gt it.first,
                        Filing::dateFiled lt it.second))
                }
                .awaitAll()
                .flatMap { it.toList() }
                .chunked(100).forEach {
                    it.mapAsync {
                        if (it.formType == "10-K") {
                            it.copy(closestYearReportId = it._id)
                        } else {
                            it.copy(closestYearReportId = getClosestAnnualReportId(it))
                        }
                    }.awaitAll().toList().forEach {
                        Database.filings.updateOne(Filing::dataUrl eq it.dataUrl, it)
                    }
                }
        }
    }

    println(ms)
}

suspend fun dump10KReportsToCSVRowPerFiling() {
    val file = Paths.get("/Users/sasha/temp/10-k-filings.csv").toFile()
    val cols = listOf("ticker", "date").plus(metrics.keys).plus("dataUrl")
    val buffer = StringBuilder()

    buffer.appendLine(cols.joinToString(separator = ","))
    Database.filings.find(Filing::formType eq "10-K").consumeEach { filing ->
        val metrics = metrics.map { (_, prop) ->
            try {
                prop.get(filing)?.value?.toBigDecimal()?.toPlainString()?.toString() ?: "null"
            } catch (e: NumberFormatException) {
                e.printStackTrace()
                null
            }
        }

        buffer.appendln(
            listOf(filing.ticker, filing.dateFiled)
                .plus(metrics)
                .plus(filing.dataUrl)
                .joinToString(separator = ","))
    }

    file.writeText(buffer.toString())
}

private suspend fun dump10KReportsToCSVRowPerCompany() {
    val file = Paths.get("/Users/sasha/temp/sample.csv").toFile()

    val buffer = StringBuilder()
    buffer.appendln(metrics.flatMap { (id, prop) ->
        years.map {
            "$id$it"
        }
    }.joinToString(separator = ",", prefix = "ticker,"))

    Database.getSP500Companies().forEach {
        val allReports = it.cik.flatMap { Database.getFilingsByCik(it.toString()) }.filter { it.formType == "10-K" }

        val byFiscalYear = getByFiscalYear(allReports)

        val entry = metrics.flatMap { (id, prop) ->
            years.map {
                byFiscalYear[it]?.let {
                    prop.get(it)?.value?.toBigDecimal()?.toPlainString()?.toString() ?: "null"
                }
            }
        }.joinToString(separator = ",", prefix = "${it.primaryTicker},")
        buffer.appendln(entry)
    }

    file.writeText(buffer.toString())
}


fun getByFiscalYear(reports: List<Filing>): Map<Int, Filing> {
    return reports.groupBy { it.fiscalYear?.toInt() ?: -1 }.filterKeys { it > 0 }.mapValues { it.value.first() }
}

suspend fun fixDecimalsInSP500AnnualReports() {
    val sP500Companies = Database.getSP500Companies()
    sP500Companies.forEach {
        val filing = it.cik.flatMap {
            Database.getFilingsByCik(it.toString())
        }.filter { it.formType == "10-K" }

        filing.forEach {
            Database.filings.save(it.copy(
                assets = Assets.get(it),
                revenue = Revenue.get(it),
                eps = Eps.get(it),
                liabilities = Liabilities.get(it),
                operatingCashFlow = OperatingCashFlow.get(it),
                financingCashFlow = FinancingCashFlow.get(it),
                investingCashFlow = InvestingCashFlow.get(it),
                netIncome = NetIncome.get(it),
                fiscalYear = FiscalYearExtractor.get(it)?.toLong()
            ))
        }
    }
}

suspend fun resolveFiles() {
    Database.filings.find("{files: null}").batchSize(10000).toList().chunked(20).forEach {
        val (download, time) = measureTimedValue {
            it.mapAsync { it.withFiles() }.awaitAll()
        }

        println("done in $time")

        download.forEach { filing ->
            coroutineScope {
                async {
                    println("saving ${filing.companyName} -> ${filing.ticker}")
                    val result = Database.filings.replaceOne(filing)
                    if (!result.wasAcknowledged()) {
                        println("failed to update: $result")
                    }
                }
            }
        }
    }
}

suspend fun resolveFilingTickers() {
    Database.filings.find("{ticker: null}").batchSize(10000).toList().chunked(1000).forEach {
        it.mapAsync { it.withTicker() }.awaitAll().forEach { filing ->
            coroutineScope {
                async {
                    val result = Database.filings.replaceOne(filing)
                    if (!result.wasAcknowledged()) {
                        println("failed to update: $result")
                    }
                }
            }
        }
    }
}

suspend fun fixDataExtractionStatus() {
    val list = Database.filings.find("{extractedData: {\$ne: null}, dataExtractionStatus: null}").toList()
    list.forEach {
        Database.filings.replaceOne(it.copy(dataExtractionStatus = OperationStatus.DONE))
    }
}

suspend fun resolveAnnualReferencesAndYearToYearDiffs(months: Long) {
    val allFilings = Database.filings.find().consumeEach {
        val filing = it
            .withClosestAnnualReportLink()
            .withYearToYearDiffs()

        val result = Database.filings.replaceOne(filing)
        if (!result.wasAcknowledged()) {
            println("failed to update: $result")
        } else {
            println("updated: ${filing.companyName} from ${filing.dateFiled}")
        }
    }
}

suspend fun fixDataExtraction(months: Long) {
    val allFilings = Database.getAllFilings()
//    val allFilings = Database.filings.find("{ticker: 'AAPL'}").toList()
    runOnComputationThreadPool {
        allFilings.chunked(150).forEach {
            it.mapAsync {
                val filing = it
                    .withBasicFilingData()
                    .withExtractedMetrics()
//                    .withClosestAnnualReportLink()
//                    .withYearToYearDiffs()
                val result = Database.filings.replaceOne(filing)
                if (!result.wasAcknowledged()) {
                    println("failed to update: $result")
                } else {
                    println("updated: ${filing.companyName} from ${filing.dateFiled}")
                }
            }.awaitAll()
        }
    }
}

