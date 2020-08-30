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
import java.lang.Exception
import java.nio.file.Paths
import java.time.LocalDate
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.measureTimeMillis
import kotlin.time.measureTimedValue

/**
 * Take the data from sec (json file extract) and company info from internet.
 * Sec data is considered as primary (has more actual ticker and sic numbers).
 * The algorithms collects all the records that have the same sic number or the same
 * ticker and aggregates them into a single record.
 */
suspend fun consolidateCompanyInfo() {
    @Serializable
    data class SecCompanyRecord(
        val cik_str: Int,
        val title: String,
        val ticker: String)

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
//        fixDataExtraction()
//        resolveCashIncomeForAnnualFilings()
        dump10KReportsToCSVRowPerFiling()
    }
}


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
    val years = 2011..2019
    val metrics = mapOf(
        "revenue" to Filing::revenue,
        "netIncome" to Filing::netIncome,
        "eps" to Filing::eps,
        "assets" to Filing::assets,
        "financingCashFlow" to Filing::financingCashFlow,
        "investingCashFlow" to Filing::investingCashFlow,
        "operatingCashFlow" to Filing::operatingCashFlow,
        "equity" to Filing::equity,
        "liabilities" to Filing::liabilities)


    val file = Paths.get("/Users/sasha/temp/10-k-filings.csv").toFile()
    val cols = listOf("ticker", "date", "fiscalYear", "sharesOutstanding", "reconcileNetIncomeToCashflow").plus(metrics.keys).plus("dataUrl")
    val buffer = StringBuilder()

    val liveCompanies = Database.getLiveCompanies()
    val blueCaps = Database.getSP500Companies()
    val isBlueCap: (Filing) -> Boolean = { filing ->
        blueCaps.any { it.cik.contains(filing.cik?.toInt()) || it.tickers.contains(filing.ticker) }
    }

    var missRate = 0L;
    var successRate = 0L;
    buffer.appendLine(cols.joinToString(separator = ","))
    Database.filings.find(
        and(Filing::formType eq "10-K")
    ).toList()
        .filter { it.revenue?.value?.let { it > 1000000000} ?: true }
//        .filter { liveCompanies.contain(it) }
        .filter { isBlueCap(it) }
        .sortedByDescending { it.revenue?.value }
        .map { filing ->
            val metrics = metrics.map { (_, prop) ->
                try {
                    val value = prop.get(filing)
                        ?.value
                        ?.toBigDecimal()
                        ?.toPlainString()

                    if (value != null) {
                        successRate++
                    } else {
                        missRate++
                    }
                    value ?: "null"
                } catch (e: NumberFormatException) {
                    missRate++;
                    null
                }
            }

            val adjustmentsToReconcileNetIncomeToCashflow = try {
                filing.cashIncome?.toBigDecimal()?.toPlainString()
            } catch (e: java.lang.NumberFormatException) {
                println("failed to parse ${filing.cashIncome}")
                null
            }
        buffer.appendln(
            listOf(filing.ticker, filing.dateFiled, filing.fiscalYear, filing.sharesOutstanding, adjustmentsToReconcileNetIncomeToCashflow)
                .plus(metrics)
                .plus(filing.dataUrl)
                .joinToString(separator = ","))
    }

    println("miss rate: $missRate")
    println("success rate: $successRate")
    file.writeText(buffer.toString())
}

suspend fun resolveFiles() {
    Database.filings.find("{files: null}").batchSize(10000).toList().chunked(20).forEach {
        val (download, time) = measureTimedValue {
            it.mapAsync { it.withFiles() }.awaitAll()
        }

        println("done in $time")

        download.forEach { filing ->
            coroutineScope {
                    println("saving ${filing.companyName} -> ${filing.ticker}")
                    val result = Database.filings.replaceOne(filing)
                    if (!result.wasAcknowledged()) {
                        println("failed to update: $result")
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

suspend fun resolveCashIncomeForAnnualFilings() {
    val allFilings = Database.getAllFilings("{formType: '10-K'}}")
    counter.set(0)
    runOnComputationThreadPool {
        allFilings.forEachAsync {
            it.consumeEach { filing ->
                val cashIncome = calculateReconciliationValues(filing)
                println("Cash income for ${filing.dataUrl}/${filing.files?.cashFlow} is $cashIncome")
                val updatedFiling = filing.copy(cashIncome = cashIncome)
                val result = Database.filings.replaceOne(updatedFiling)
                if (!result.wasAcknowledged()) {
                    println("${counter.incrementAndGet()} failed to update: $result")
                } else {
                    println("${counter.incrementAndGet()} updated: ${updatedFiling.companyName} from ${updatedFiling.dateFiled}")
                }
            }
        }
    }
}

val counter = AtomicInteger(0)
suspend fun resolveAnnualReferencesAndYearToYearDiffs(months: Long) = runOnComputationThreadPool {
    Database.getAllFilings().mapAsync {
        it.consumeEach {
            val filing = it
//                .withExtractedMetrics()
                .withClosestAnnualReportLink()
                .withYearToYearDiffs()

            val result = Database.filings.replaceOne(filing)
            if (!result.wasAcknowledged()) {
                println("failed to update: $result")
            } else {

                println("[${counter.incrementAndGet()} updated: ${filing.companyName} from ${filing.dateFiled}")
            }
        }
    }
}

suspend fun fixDataExtraction() {
    val allFilings = Database.getAllFilings("{formType: '10-K'}")
    counter.set(0)
    runOnComputationThreadPool {
        allFilings.forEachAsync {
            it.consumeEach {
                val filing =
                    it
                        //.copy(dataExtractionStatus = OperationStatus.PENDING)
//                        .withTicker()
//                        .withBasicFilingData()
                        .withExtractedMetrics()
                val result = Database.filings.replaceOne(filing)
                if (!result.wasAcknowledged()) {
                    println("${counter.incrementAndGet()} failed to update: $result")
                } else {
                    println("${counter.incrementAndGet()} updated: ${filing.companyName} from ${filing.dateFiled}")
                }
            }
        }
    }
}

