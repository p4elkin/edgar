package fi.avp.edgar.mining

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
    val financialSummary: String?)

private val xmlXBRLReportPattern = Regex("(.*)_cal.xml")
suspend fun fetchRelevantFileNames(reportReference: ReportReference): ReportFiles {
    val reportBaseUrl = reportReference.dataUrl
    val index = asyncJson("$reportBaseUrl/index.json")

    val reportNode = index["directory"]["item"]
        .filter { it.text("name").endsWith(".htm") }
        .filter { (!it.text("name").contains("ex")) }
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
    Database.getSP500Companies().filter { it.tickers.contains("INTC") }.forEach {
        if (!Files.exists(Paths.get("${Locations.reports}/${it.primaryTicker}.zip"))) {
            val primaryTicker = it.primaryTicker
            println("Downloading $primaryTicker")
            downloadReports(
                primaryTicker,
                it.tickers.flatMap { Database.getReportReferencesByTicker(it) })
        }
    }
}

fun downloadReports(ticker: String, refs: List<ReportReference>) {
    println("Downloading ${ticker}")
            runBlocking {
                val downloadedStuff = refs
                    .sortedByDescending { it.dateFiled }
                    .filter { it.reportFiles != null }
                    .distinctBy { it.reportFiles!!.xbrlReport }
                    .mapAsync { downloadSingleReport(it) }
                    .awaitAll()

                if (downloadedStuff.isNotEmpty()) {
                    saveZippedReports(ticker, downloadedStuff.filterNotNull())
                    delay(1000)
                }
            }
}

suspend fun downloadSingleReport(reportReference: ReportReference): XBRL? {
    var result: XBRL?
    for (trialIndex in 0..2) {
        try {
            val (xbrl, income, cashflow, balance) = listOf(
                reportReference.reportFiles?.xbrlReport,
                reportReference.reportFiles?.income,
                reportReference.reportFiles?.cashFlow,
                reportReference.reportFiles?.balance)
                .mapAsync {
                    it?.let {
                        asyncGetText("${reportReference.dataUrl}/$it")
                    }
                }.awaitAll()

            result = XBRL(
                reportReference.reportFiles!!.xbrlReport!!,
                xbrl,
                cashflow,
                balance,
                income)

            return result
        } catch (e: Exception) {
            println("Failed to parse ${reportReference.reportFiles?.xbrlReport} due to ${e.message}, retrying")
        }
    }

    println("Failed to parse ${reportReference.reportFiles?.xbrlReport} after 3 attempts")

    return null
}

fun saveReportWithoutZipping(ticker: String, data: XBRL) {
    val path = Paths.get("${Locations.reportsExtracted}/${ticker}.zip")
    if (!Files.exists(path)) {
        Files.createDirectory(path)
    }

    val createFile: (String, String) -> Unit = { fileName, text ->
        if (!Files.exists(path.resolve(fileName))) {
            val file = Files.createFile(path.resolve(fileName)).toFile()
            file.writeText(text, StandardCharsets.UTF_8)
        }
    }

    data.cashFlow?.let {
        createFile("cashflow-" + data.reportFileName, it)
    }

    data.balanceSheet?.let {
        createFile("balance-" + data.reportFileName, it)
    }

    data.incomeStatement?.let {
        createFile("income-" + data.reportFileName, it)
    }

    data.xbrl?.let {
        createFile(data.reportFileName, it)
    }
}

fun saveZippedReports(ticker: String, xbrls: List<XBRL>) {
    val path = "${Locations.reports}/${ticker}.zip"
    if (Files.exists(Paths.get(path))) {
        Files.delete(Paths.get(path))
    }

    ZipOutputStream(BufferedOutputStream(FileOutputStream(path))).use { out ->
        xbrls.forEach { entry ->
            createZipEntry(entry.reportFileName, "", entry.xbrl, out)
            createZipEntry(
                entry.reportFileName,
                "cashflow-",
                entry.cashFlow,
                out
            )
            createZipEntry(
                entry.reportFileName,
                "balance-",
                entry.balanceSheet,
                out
            )
            createZipEntry(
                entry.reportFileName,
                "income-",
                entry.incomeStatement,
                out
            )
        }
    }
}

fun createZipEntry(reportFileName: String, prefix: String, data: String?, out: ZipOutputStream) {
    data?.let {
        BufferedInputStream(it.byteInputStream(StandardCharsets.UTF_8)).use { origin ->
            val zipEntry = ZipEntry("${prefix}$reportFileName")
            out.putNextEntry(zipEntry)
            origin.copyTo(out, 1024)
        }
    }
}

fun main() {
    downloadXBRL()
}
