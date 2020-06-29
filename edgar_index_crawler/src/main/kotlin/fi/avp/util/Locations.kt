package fi.avp.util

import fi.avp.edgar.data.ReportMetadata
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream

object Locations {

 val parentDir: Path = Paths.get("").toAbsolutePath().parent

 val xbrlDir: Path = parentDir.resolve("data/xbrl")

 val reports: Path = parentDir.resolve("data/reports")

 val indicesDir: Path = parentDir.resolve("data/report_indices")

}

fun getReportData(ticker: String): ZipInputStream {
 val zipLocation = Locations.reports.resolve("${ticker}.zip")
 return ZipInputStream(BufferedInputStream(FileInputStream(zipLocation.toFile())))
}

fun companyQuarterlyReport(reportMetadata: ReportMetadata): Path? {
 val quarterReportPath = Locations.xbrlDir
  .resolve(reportMetadata.companyRef.name)
  .resolve(reportMetadata.year)
  .resolve(reportMetadata.quarter)

  if (!Files.exists(quarterReportPath)) {
   return null;
  }

 val companyName = reportMetadata.companyRef.name
 val reportCandidateBlackList = Regex(".*_(cal|pre|def|lab)\\..*")
 return Files.list(quarterReportPath)
  .filter { !reportCandidateBlackList.matches(it.fileName.toString())}
  .filter {
    val reportFileName = it.fileName.toString()
    reportFileName.endsWith("xml") ||
            (reportFileName.endsWith("htm") &&
                    // Filter out potentially large extra files
                    (companyName.contains("ex") || !reportFileName.contains("ex")))
  }
  .max(compareBy {Files.size(it)}).orElse(null)
}
