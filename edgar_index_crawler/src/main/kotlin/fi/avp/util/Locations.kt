package fi.avp.util

import fi.avp.edgar.data.ReportRecord
import jdk.internal.dynalink.linker.ConversionComparator
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

object Locations {

 val parentDir: Path = Paths.get("").toAbsolutePath().parent

 val xbrlDir: Path = parentDir.resolve("data/xbrl")

 val indicesDir: Path = parentDir.resolve("data/report_indices")

}

fun companyQuarterlyReport(reportRecord: ReportRecord): Path? {
 val quarterReportPath = Locations.xbrlDir
  .resolve(reportRecord.companyRef.name)
  .resolve(reportRecord.year)
  .resolve(reportRecord.quarter)

  return Files.list(quarterReportPath).max(compareBy {Files.size(it)}).orElse(null)
}
