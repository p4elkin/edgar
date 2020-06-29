package fi.avp.edgar

import fi.avp.util.*
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream


data class ReportFiles(
    val visualReport: String?,
    val xbrlReport: String?,
    val cashFlow: String?,
    val income: String?,
    val operations: String?,
    val balance: String?,
    val financialSummary: String?
)

private val xmlXBRLReportPattern = Regex("(.*)_cal.xml")
suspend fun fetchRelevantFileNames(reportReference: ReportReference): ReportFiles {
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

fun downloadXBRL() {
    Database.reportIndex.find().toList().groupBy { it.ticker }
        .filter { it.key != null && !Files.exists(Paths.get("${Locations.reports}/${it.key}.zip")) }
        .forEach { companyReports ->
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

suspend fun download(reportReference: ReportReference): XBRL? {
    return reportReference.reportFiles?.xbrlReport?.let {
        XBRL(reportReference.dataUrl!!, asyncGetText("${reportReference.dataUrl}/$it"))
    }
}

fun saveReports(ticker: String, xbrls: List<XBRL>) {
    ZipOutputStream(BufferedOutputStream(FileOutputStream("${Locations.reports}/${ticker}.zip"))).use { out ->
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
