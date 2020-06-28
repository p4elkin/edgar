package fi.avp.edgar

import com.mongodb.client.MongoCollection
import fi.avp.util.*
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.litote.kmongo.find
import org.litote.kmongo.save
import org.litote.kmongo.updateOne
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

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

fun downloadReports() {
    val reports = Database.reportIndex.find().toList()
    reports.groupBy { it.ticker }.filter { it.key != null && !Files.exists(Paths.get("/Users/sasha/Desktop/reports/${it.key}.zip")) }.forEach { companyReports ->
        println("Downloading ${companyReports.key}")
        repeat(3) {
            try {
                runBlocking {
                    val downloadedStuff = companyReports.value.filter { it.reportFiles != null }
                        .mapAsync { reportReference ->
                            download(reportReference)
                        }.awaitAll()
                        .filterNotNull()

                    if (downloadedStuff.isNotEmpty()) {
                        saveReports(companyReports.key!!, downloadedStuff)
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

fun saveReports(ticker: String, xbrls: List<XBRL>) {
    ZipOutputStream(BufferedOutputStream(FileOutputStream("/Users/sasha/Desktop/reports/${ticker}.zip"))).use { out ->
        xbrls.forEach { entry ->
            entry.xbrl?.let {
                BufferedInputStream(it.byteInputStream(StandardCharsets.UTF_8)).use { origin ->
                    val zipEntry = ZipEntry("${entry.dataUrl.substringAfterLast("/")}.xml")
                    out.putNextEntry(zipEntry)
                    origin.copyTo(out, 1024)
                }

            }
        }
    }
}

fun __amendFinancingOperationRelatedMetrics(collection: MongoCollection<ReportRecord>) {
    getCompanyNames().filter { it == "apple_inc" }.forEach {
        val escapedName = it.replace("\\", "\\\\")
        collection.find("{name: '$escapedName'}").forEach { report ->
            if (report.metrics == null) {
                println("${report.name} ${report._id} ${report.dataUrl}")
            }
            val updatedMetrics = report.metrics?.map {
                if (it.sourcePropertyName == "NetCashProvidedByUsedInFinancingActivitiesContinuingOperations" ||
                    it.sourcePropertyName == "NetCashProvidedByUsedInFinancingActivities") {
                    it.copy(category = "cashFlow", type = "financingCashFlow")
                } else {
                    it
                }
            }

            collection.updateOne(report.copy(metrics = updatedMetrics))
        }
    }
}

suspend fun download(reportReference: ReportReference): XBRL? {
    return reportReference.reportFiles?.xbrlReport?.let {
        XBRL(reportReference.dataUrl!!, asyncGetText("${reportReference.dataUrl}/$it"))
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
//    resolveFileNames()
    downloadReports()
}

