package fi.avp.edgar.util

import java.io.File
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.zip.ZipInputStream

const val EDGAR_DATA = "https://www.sec.gov/Archives/edgar/data/"
const val EDGAR_INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index/"
const val REPORT_INDEX_FILE_NAME = "report_index.csv"
const val DAILY_INDEX = "https://www.sec.gov/Archives/edgar/daily-index/"

object Locations {

 val parentDir: Path = Paths.get("").toAbsolutePath().parent

 val reports: Path = parentDir.resolve("data/reports")

 val quotes: Path = parentDir.resolve("data/quotes")

 val reportsExtracted: Path = parentDir.resolve("data/reports/extracted")

 val splitData = parentDir.resolve("data/split_yfinance")
}

fun getReportData(ticker: String): Map<String, InputStream>? {
 val dirName = "${ticker}.zip"
 val dataPath = Locations.reportsExtracted.resolve(dirName)
 if (!Files.exists(dataPath)) {
  return null
 }

 return Locations.reportsExtracted.resolve(dirName).toFile().listFiles()
  .filterNot { itAuxillaryFile(it) }
  .map { it.name to it.inputStream() }
  .toMap()
}

fun itAuxillaryFile(file: File): Boolean {
    return file.name.matches(Regex("^(cashflow|balance|income-).*\\.xml\$"))
}

fun getReportDataZip(ticker: String): ZipInputStream? {
 val zipLocation = Locations.reports.resolve("${ticker}.zip")
 if (!Files.exists(zipLocation)) {
     return null
 }
 return ZipInputStream(zipLocation.toFile().inputStream().buffered())
}

