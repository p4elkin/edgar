package fi.avp.edgar

import com.mongodb.client.MongoCollection
import fi.avp.edgar.data.CompanyRef
import fi.avp.edgar.data.ReportMetadata
import fi.avp.util.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.regex.Pattern

val reportEntryPattern: Pattern = Pattern.compile("(\\d+)?\\|(.+)?\\|(10-K|10-Q)?\\|(.+)?\\|(.+\\.txt)")

val sp500List = Thread.currentThread().contextClassLoader
    ?.getResource("sp500.txt")
    ?.readText()
    ?.lines() ?: emptyList()


fun main() {
    val reportCollection: MongoCollection<ReportRecord> = Database.database.getCollection("reports", ReportRecord::class.java)
}

private data class ReportData(
    val metadata: ReportMetadata,
    val extracts: Map<String, String>,
    val reportFile: String,
    val xbrlData: InputStream?) {

    fun toRecord() = ReportRecord(
        metadata.companyRef.cik,
        metadata.companyRef.name,
        metadata.getReportId(),
        metadata.reportType,
        metadata.date.toLocalDate(),
        metadata.getReportDataURL(),
        reportFile,
        metrics = emptyList(),
        extracts = extracts)
}

private typealias DownloadTask = suspend CoroutineScope.() -> ReportData

private fun fetchCompanyReports(
    companyRef: CompanyRef,
    companyReportsLocation: Path,
    reportMetadataByYear: Map<String, Map<String, List<ReportMetadata>>>) : List<ReportData> {

    println("processing ${companyRef.name}")
    val companyDir = companyReportsLocation.resolve(companyRef.name)

    ensureDirectory(companyDir)

    val downloadTasks = ArrayList<DownloadTask>()
    reportMetadataByYear.forEach { (year, quarterlyReports) ->
        val yearDirectory = companyDir.resolve(year)
        ensureDirectory(yearDirectory)

        quarterlyReports.forEach { (quarterId, records) ->
            val quarterDir = yearDirectory.resolve(quarterId)
            ensureDirectory(quarterDir)
            records.forEach {
                downloadTasks.add { createDownloadTask(it, quarterDir) }
            }
        }
    }

    println("downloading ${downloadTasks.count()} documents")
    return runBlocking {
        val results = runDownloadTasks(downloadTasks)
        delay(3000)
        results
    }
}

private suspend fun runDownloadTasks(downloadTasks: ArrayList<DownloadTask>): List<ReportData> {
    return downloadTasks.mapAsync { it(this) }.awaitAll()
}

private fun saveXBRLZip(filePath: Path, reportContent: InputStream?) {
    if (Files.exists(filePath)) {
        return
    }

    val reportFile = Files.createFile(filePath)
    reportContent?.let {
        try {
            reportFile.toFile().outputStream().use {
                reportContent.copyTo(it)
                it.flush()
            }
        } catch (e: Exception) {
            e.printStackTrace()
        } finally {
            reportContent.close()
        }
    }
}


private suspend fun createDownloadTask(metadata: ReportMetadata, quarterDir: Path): ReportData {
    val reportBaseUrl = metadata.getReportDataURL()
    val index = asyncJson("$reportBaseUrl/index.json")

    val reportNode = index["directory"]["item"]
        .filter { it.text("name").endsWith(".htm") }
        .maxBy { it.long("size") }
    val humanReadableReportFileName = reportNode?.text("name") ?: ""

    val filingSummary = FilingSummary(asyncGet("$reportBaseUrl/FilingSummary.xml").byteStream())

    val reports = mapOf(
        "cashFlow" to filingSummary.getConsolidatedStatementOfCashFlow(),
        "income" to filingSummary.getConsolidatedStatementOfIncome(),
        "balance" to filingSummary.getConsolidatedBalanceSheet(),
        "operations" to filingSummary.getConsolidatedStatementOfOperation()
    ).filterValues {
        it != null
    }.mapValues {
        asyncGetText("$reportBaseUrl/${it.value!!}")
    }
//    val xbrlUrl = "$reportBaseUrl/${reportId}-xbrl.zip"
//    val report = asyncGet(xbrlUrl).byteStream()

//    val targetReportFile = quarterDir.resolve(generateRecordFileName(metadata))
//    println("downloading $xbrlUrl, waiting...")

    return ReportData(metadata, reports, humanReadableReportFileName, null)
}


private fun ensureDirectory(path: Path) {
    if (!Files.exists(path)) {
        Files.createDirectory(path)
    }
}

