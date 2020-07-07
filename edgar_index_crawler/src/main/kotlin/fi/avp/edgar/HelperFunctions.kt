package fi.avp.edgar

import fi.avp.util.mapAsync
import kotlinx.coroutines.*
import org.litote.kmongo.find
import org.litote.kmongo.getCollection
import org.litote.kmongo.save
import org.litote.kmongo.updateOne
import sun.nio.ch.Net
import java.lang.Math.min
import java.lang.StringBuilder
import java.nio.file.Paths
import java.util.concurrent.Executors
import kotlin.reflect.full.memberProperties

fun ensureCikPaddingsInTickerMappings() {
    val updated = Database.tickers.find().map {
        it.copy(cik = it.cik.padStart(10, '0'), ticker = it.ticker.toUpperCase())
    }.distinctBy { it.cik }

    Database.tickers.drop()
    Database.tickers.insertMany(updated)
}

fun ensureCikPaddingsInReport() {
    val tickers = Database.tickers.find().toList()
    val reportRefs = Database.reportIndex.find().toList()
    val updated = reportRefs.map {
        it.copy(ticker = tickers
            .find { tickerMapping -> it.cik?.padStart(10, '0') == tickerMapping.cik}?.ticker)
    }

    Database.reportIndex.drop()
    Database.reportIndex.insertMany(updated)
}

fun resolveFileNames(reports: List<ReportReference>) {
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

val revenueProps = listOf(
    "Revenues",
    "SalesRevenueNet",
    "TotalRevenuesAndOtherIncome",
    "SalesRevenueGoodsNet",
    "SalesRevenueServicesNet",
    "OilAndGasRevenue",
    "FoodAndBeverageRevenue",
    "SalesRevenueServicesGross",
    "ElectricUtilityRevenue",
    "SegmentReportingInformationRevenue",
    "RevenuesNetOfInterestExpense",
    "ElectricalTransmissionAndDistributionRevenue",
    "RealEstateRevenueNet",
    "HealthCareOrganizationRevenue",
//    "BrokerageCommissionsRevenue",
    "RefiningAndMarketingRevenue",
    "RevenueMineralSales",
    "AdministrativeServicesRevenue",
    "HomeBuildingRevenue",
    "RevenuesExcludingInterestAndDividends",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenuesExcludingCorporate"//https://www.sec.gov/Archives/edgar/data/1015780/000119312513204379/0001193125-13-204379-index.html
)

val income = listOf(
    "OperatingIncomeLoss",
    "GrossProfit"
)

val eps = listOf(
    "EarningsPerShareDiluted",
    "EarningsPerShareBasicAndDiluted",
    "EarningsPerShareBasic",
    "IncomeLossFromContinuingOperationsPerDilutedShare",
    "IncomeLossFromContinuingOperationsPerBasicShare",
    "fast_BasicDilutedEarningsPerShareNetIncome"
    //WeightedAverageNumberOfDilutedSharesOutstanding
    //WeightedAverageNumberBasicDilutedSharesOutstanding
// EarningsPerShareDiluted
)


fun moveSP500Reports() {
    val tickers = Database.getSP500Tickers()
    val sp500 = Database.database.getCollection("sp500", ReportReference::class.java)
    Database.reportIndex.find().toList().filter { it.ticker in tickers }.forEach {
        sp500.save(it)
    }
}

fun findReportsLackingContext() {
    Database.database.getCollection("sp500", ReportReference::class.java).find().forEach {
        if (it.contexts == null || it.contexts.size < 2) {
            println(it.dataUrl)
        }
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

fun locateTickerMismatch() {
    val reports = Database.database.getCollection<ReportReference>("sp500")
}

fun printSP500Tickers() {
    Database.getSP500Tickers().forEach {
        println(it)
    }
}

fun main() {

//    printSP500Tickers()
    dump10KReportsToCSV()

//    moveSP500Reports()
//    fixReportRefsPointingToExtracts()
//    findReportsWithMultipleEPS()
//    generateReport()
//    findReportsLackingContext()
//    calculateAssets {it.copy(
//        assets = Assets.get(it),
//        revenue = Revenue.get(it),
//        eps = Eps.get(it),
//        liabilities = Liabilities.get(it),
//        operatingCashFlow = OperatingCashFlow.get(it),
//        financingCashFlow = FinancingCashFlow.get(it),
//        investingCashFlow = InvestingCashFlow.get(it),
//        netIncome = NetIncome.get(it),
//        fiscalYear = FiscalYearExtractor.get(it)?.toLong())}


}

private fun dump10KReportsToCSV() {
    val file = Paths.get("/Users/sasha/temp/sample.csv").toFile()

    val buffer = StringBuilder()
    buffer.appendln(metrics.flatMap { (id, prop) ->
        years.map {
            "$id$it"
        }
    }.joinToString(separator = ",", prefix = "ticker,"))

    Database.getSP500Tickers().forEach {
        val reports = Database.database.getCollection<ReportReference>("sp500")
        val allReports = reports
            .find("{ticker: '$it'}")
            .batchSize(10000).toList()

        val byFiscalYear = getByFiscalYear(allReports)

        val entry = metrics.flatMap { (id, prop) ->
            years.map {
                byFiscalYear[it]?.let {
                    prop.get(it)?.value?.toBigDecimal()?.toPlainString()?.toString() ?: "null"
                }
            }
        }.joinToString(separator = ",", prefix = "$it,")
        buffer.appendln(entry)
    }

    file.writeText(buffer.toString())
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

fun getByFiscalYear(reports: List<ReportReference>): Map<Int, ReportReference> {
    return reports.groupBy { it.fiscalYear?.toInt() ?: -1 }.filterKeys { it > 0 }.mapValues { it.value.first() }
}

private fun fixReportRefsPointingToExtracts() {
    val reports = Database.reportIndex.find("{'reportFiles.visualReport': {\$regex : '.*ex.*'}}").toList()
    resolveFileNames(reports)
}

