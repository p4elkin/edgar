package fi.avp.edgar

import fi.avp.edgar.data.ReportMetadata
import fi.avp.util.*
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.litote.kmongo.Data
import org.litote.kmongo.find
import org.litote.kmongo.save
import java.nio.file.Path

fun ensureCikPaddingsInTickerMappings() {
    val updated = Database.ticker.find().map {
        it.copy(cik = it.cik.padStart(10, '0'), ticker = it.ticker.toUpperCase())
    }.distinctBy { it.cik }

    Database.ticker.drop()
    Database.ticker.insertMany(updated)
}

fun ensureCikPaddingsInReport() {
    val tickers = Database.ticker.find().toList()
    val reportRefs = Database.reportIndex.find().toList()
    val updated = reportRefs.map {
        it.copy(ticker = tickers
            .find { tickerMapping -> it.cik?.padStart(10, '0') == tickerMapping.cik}?.ticker)
    }

    Database.reportIndex.drop()
    Database.reportIndex.insertMany(updated)
}

fun resolveFileNames() {
    val reports = Database.reportIndex.find().toList()
    reports.groupBy { it.ticker }.filter { it.key != null }.forEach { companyReports ->
        println("Downloading ${companyReports.key}")
        repeat(3) {
            try {
                runBlocking {
                    val downloadedStuff = companyReports.value.filter { it.reportFiles == null }
                        .mapAsync {
                            it.copy(reportFiles = downloadTask(it))
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

data class ReportFiles(
    val visualReport: String?,
    val xbrlReport: String?,
    val cashFlow: String?,
    val income: String?,
    val operations: String?,
    val balance: String?,
    val financialSummary: String?
)

val xmlXBRLReportPattern = Regex("(.*)_cal.xml")
suspend fun downloadTask(reportReference: ReportReference): ReportFiles {
    val reportBaseUrl = reportReference.dataUrl
    val index = asyncJson("$reportBaseUrl/index.json")

    val reportNode = index["directory"]["item"]
        .filter { it.text("name").endsWith(".htm") }
        .maxBy { it.long("size") }

    val calFile = index["directory"]["item"]
        .find { it.text("name").endsWith("_cal.xml") }
        ?.text("name") ?: ""

    val humanReadableReportFileName = reportNode?.text("name") ?: ""
    val xbrlFile = xmlXBRLReportPattern.find(calFile)?.groups?.get(1)?.let { matchGroup ->
        index["directory"]["item"].find { it.text("name").endsWith("${matchGroup.value}.xml") }
            ?.text("name")
    } ?: humanReadableReportFileName

    val filingSummary = try {
        FilingSummary(asyncGet("$reportBaseUrl/FilingSummary.xml").byteStream())
    } catch (e: Exception) {
        null
    }

    return ReportFiles(
        humanReadableReportFileName,
        xbrlFile,
        filingSummary?.getConsolidatedStatementOfCashFlow(),
        filingSummary?.getConsolidatedStatementOfIncome(),
        filingSummary?.getConsolidatedStatementOfOperation(),
        filingSummary?.getConsolidatedBalanceSheet(),
        filingSummary?.getFinancialSummary()
    )
}

fun updateDate() {
    val reports = Database.reportIndex.find().toList()
    Database.reportIndex.drop()
    Database.reportIndex.insertMany(reports)
}

fun updateDataUrl() {
    val reports = Database.reportIndex.find().toList()
    val backup = Database.database.getCollection("report-index-bak", ReportReference::class.java)

    backup.drop()
    backup.insertMany(reports)

    Database.reportIndex.drop()
    Database.reportIndex.insertMany(reports)
}

fun main() {
    resolveFileNames()
}

