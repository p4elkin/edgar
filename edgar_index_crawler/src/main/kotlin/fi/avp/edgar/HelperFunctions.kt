package fi.avp.edgar

import fi.avp.edgar.mining.*
import fi.avp.util.mapAsync
import kotlinx.coroutines.*
import org.litote.kmongo.*
import java.lang.StringBuilder
import java.nio.file.Paths

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
fun consolidateCompanyInfo() {
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

    val companyInfoDatabase = Database.database.getCollection("company-info").find().toList()

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

    val sP500Tickers = Database.getSP500Tickers()
    companyList.find().toList().forEach {
        var tickers = it.tickers
            .flatMap { setOf(it, it.replace('-', '.'), it.replace('.', '-')) }.toSet()

        if (it.tickers.contains("GOOG")) {
           tickers = tickers.plus("GOOGL")
        }

        if (it.tickers.any { sP500Tickers.contains(it) }) {
            companyList.replaceOne(it.copy(isInSP500 = true))
        }
        companyList.replaceOne(it.copy(tickers = tickers))
    }
}

fun moveSP500Reports() {
    val tickers = Database.getSP500Tickers()
    val sp500 = Database.database.getCollection("sp500", ReportReference::class.java)
    Database.reportIndex.find().toList().filter { it.ticker in tickers }.forEach {
        sp500.save(it)
    }
}

fun findReportsWithMultipleEPS() {
    val reports = Database.database.getCollection<ReportReference>("sp500")
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
    dump10KReportsToCSVRowPerFiling()
//    fixDecimalsInSP500AnnualReports()
//    consolidateCompanyInfo()
}

val years = 2011..2019
val metrics = mapOf(
    "revenue" to ReportReference::revenue,
    "netIncome" to ReportReference::netIncome,
    "eps" to ReportReference::eps,
    "assets" to ReportReference::revenue,
    "financingCashFlow" to ReportReference::financingCashFlow,
    "investingCashFlow" to ReportReference::investingCashFlow,
    "operatingCashFlow" to ReportReference::operatingCashFlow,
    "liabilities" to ReportReference::liabilities)

fun dump10KReportsToCSVRowPerFiling() {
    val file = Paths.get("/Users/sasha/temp/10-k-filings.csv").toFile()
    val cols = listOf("ticker", "date").plus(metrics.keys).plus("dataUrl")
    val buffer = StringBuilder()

    buffer.appendln(cols.joinToString(separator = ","))
    Database.getSP500Companies().forEach {companyInfo ->
        val allReports = companyInfo.cik
            .flatMap { Database.getReportReferencesByCik(it.toString()) }
            .filter { it.formType == "10-K" }

        allReports.forEach { filing ->
            val metrics = metrics.map { (_, prop) ->
                prop.get(filing)?.value?.toBigDecimal()?.toPlainString()?.toString() ?: "null"
            }

            buffer.appendln(
                listOf(companyInfo.primaryTicker, filing.dateFiled)
                    .plus(metrics)
                    .plus(filing.dataUrl)
                    .joinToString(separator = ","))
        }
    }

    file.writeText(buffer.toString())
}

private fun dump10KReportsToCSVRowPerCompany() {
    val file = Paths.get("/Users/sasha/temp/sample.csv").toFile()

    val buffer = StringBuilder()
    buffer.appendln(metrics.flatMap { (id, prop) ->
        years.map {
            "$id$it"
        }
    }.joinToString(separator = ",", prefix = "ticker,"))

    Database.getSP500Companies().forEach {
        val allReports = it.cik.flatMap { Database.getReportReferencesByCik(it.toString()) }.filter { it.formType == "10-K" }

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


fun getByFiscalYear(reports: List<ReportReference>): Map<Int, ReportReference> {
    return reports.groupBy { it.fiscalYear?.toInt() ?: -1 }.filterKeys { it > 0 }.mapValues { it.value.first() }
}

fun fixDecimalsInSP500AnnualReports() {
    val sP500Companies = Database.getSP500Companies()
    sP500Companies.forEach {
        val reportReferences = it.cik.flatMap {
            Database.getReportReferencesByCik(it.toString())
        }.filter { it.formType == "10-K" }

        reportReferences.forEach {
            Database.reportIndex.save(it.copy(
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

private fun fixReportRefsPointingToExtracts() {
    val reports = Database.reportIndex.find("{'reportFiles.visualReport': {\$regex : '.*ex.*'}}").toList()
    reports.groupBy { it.ticker }.filter { it.key != null }.forEach { companyReports ->
        println("Downloading ${companyReports.key}")
        repeat(3) {
            try {
                runBlocking {
                    val downloadedStuff = companyReports.value
                        .mapAsync {
                            it.copy(reportFiles = fetchRelevantFileNames(it))
                        }.awaitAll()

                    downloadedStuff.forEach { Database.reportIndex.save(it) }
                    if (downloadedStuff.isNotEmpty()) {
                        delay(1000)
                    }
                }
                return@forEach
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }
}

