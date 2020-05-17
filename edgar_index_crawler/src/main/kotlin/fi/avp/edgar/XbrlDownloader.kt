package fi.avp.edgar

import fi.avp.util.asyncGet
import fi.avp.util.asyncGetText
import fi.avp.util.mapAsync
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.regex.Pattern
import java.util.stream.Collectors.toList

val reportEntryPattern: Pattern = Pattern.compile("(\\d+)?\\|(.+)?\\|(10-K|10-Q)?\\|(.+)?\\|(.+\\.txt)")

val sp500List = Thread.currentThread().contextClassLoader
    ?.getResource("sp500.txt")
    ?.readText()
    ?.lines() ?: emptyList()

data class CompanyRef(val cik: String,
                      val name: String)

data class ReportRecord(val companyRef: CompanyRef,
                        val year: String,
                        val quarter: String,
                        val reportType: String,
                        val date: String,
                        val reportPath: String);

fun main(args: Array<String>) {
    val parentDir = Paths.get("").toAbsolutePath().parent
    val reportIndexLocation = parentDir.resolve("data/report_indices")
    val records = preparePerCompanyReportStorageStructure(reportIndexLocation)
    val companyReportsLocation = parentDir.resolve("data/xbrl")

    ensureDirectory(companyReportsLocation)

    records.forEach { (companyRef, yearlyReports) ->
        fetchCompanyReports(companyRef, companyReportsLocation, yearlyReports)
    }
}

private fun fetchCompanyReports(
    companyRef: CompanyRef,
    companyReportsLocation: Path,
    yearlyReports: Map<String, Map<String, List<ReportRecord>>>) {

    println("processing ${companyRef.name}")
    val companyDir = companyReportsLocation.resolve(companyRef.name)
    if (Files.exists(companyDir)) {
        println("data exists skipping...")
        return
    }

    ensureDirectory(companyDir)

    val downloadTasks = ArrayList<suspend CoroutineScope.() -> Pair<Path, InputStream>>()
    yearlyReports.forEach { (year, quarterlyReports) ->
        val yearDirectory = companyDir.resolve(year)
        ensureDirectory(yearDirectory)

        quarterlyReports.forEach { (quarterId, records) ->
            val quarterDir = yearDirectory.resolve(quarterId)
            ensureDirectory(quarterDir)
            records.forEach { record ->
                downloadTasks.add {
                    val reportId = record.reportPath.let {
                        val id = it.substringAfterLast("/")
                        id.substring(0, id.length - 4)
                    }

                    val xbrlZipResourcePath = "${reportId.replace("-", "")}/${reportId}-xbrl.zip"
                    val xbrlUrl = "${EDGAR_DATA}/${record.companyRef.cik}/${xbrlZipResourcePath}"
                    val report = asyncGet(xbrlUrl).byteStream()

                    val targetReportFile = quarterDir.resolve(generateRecordFileName(record))
                    println("downloading $xbrlUrl, waiting...")

                    return@add targetReportFile to report
                }
            }
        }
    }

    println("downloading ${downloadTasks.count()} documents")
    runBlocking {
        var progress = 0
        downloadTasks
            .groupBy { downloadTasks.indexOf(it) / 50 }
            .forEach {
                it.value
                    .mapAsync { it(this) }
                    .awaitAll()
                    .forEach { (filePath, reportContent) ->
                        try {
                            val outputStream = Files.createFile(filePath).toFile().outputStream()
                            reportContent.copyTo(outputStream)
                            outputStream.flush()
                        } catch (e: Exception) {
                            e.printStackTrace()
                        }
                    }

                progress += it.value.size
                println("downloaded $progress, waiting...")
                delay(3000)
            }
    }
    println()
}

private fun ensureDirectory(path: Path) {
    if (!Files.exists(path)) {
        Files.createDirectory(path)
    }
}

private fun preparePerCompanyReportStorageStructure(reportIndexLocation: Path): Map<CompanyRef, Map<String, Map<String, List<ReportRecord>>>> {
    val cikToCompanyName:  MutableMap<String, String> = HashMap()
    return Files.newDirectoryStream(reportIndexLocation)
        .filter { Files.isDirectory(it) }
        .flatMap { yearIndexDirectory ->
            Files.newDirectoryStream(yearIndexDirectory)
                .filter { Files.isDirectory(it) }
                .map { quarterDirectory ->
                    Files.newBufferedReader(quarterDirectory.resolve(REPORT_INDEX_FILE_NAME))
                        .lines()
                        .map { reportEntryPattern.matcher(it) }
                        .filter { it.matches() }
                        .map {
                            val cik = it.group(1).padStart(10, '0');
                            val name = cikToCompanyName.getOrPut(cik, { sanitiseCompanyName(it.group(2)) })
                            ReportRecord(
                                year = yearIndexDirectory.fileName.toString(),
                                quarter = quarterDirectory.fileName.toString(),
                                companyRef = CompanyRef(cik, name),
                                reportType = it.group(3),
                                date = it.group(4),
                                reportPath = it.group(5)
                            )
                        }.collect(toList())
                }
        }
        .flatten()
        .sortedBy { it.companyRef.name }
        .groupBy { it.companyRef }
        .mapValues {
            it.value
                .groupBy { it.year }
                .mapValues {
                    it.value
                        .groupBy { it.quarter }
                }
        }
        .filterKeys { sp500List.contains(it.cik) }
}

private fun sanitiseCompanyName(it: String) =
    it.replace(Regex("[\\.\\,\\'\\/]"), "")
      .replace(" ", "_")
      .toLowerCase()

fun generateRecordFileName(record: ReportRecord): String =
    "${record.companyRef.name}-${record.reportType}-${record.quarter}-${record.date}${record.reportPath.substringAfterLast("/")}".replace(".txt", "-xbrl.zip")

