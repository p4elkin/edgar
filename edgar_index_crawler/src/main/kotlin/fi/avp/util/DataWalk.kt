package fi.avp.util

import fi.avp.edgar.REPORT_INDEX_FILE_NAME
import fi.avp.edgar.data.CompanyRef
import fi.avp.edgar.data.ReportMetadata
import fi.avp.edgar.reportEntryPattern
import fi.avp.edgar.sp500List
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDate
import java.util.stream.Collectors


fun preparePerCompanyReportStorageStructure(reportIndexLocation: Path = Locations.indicesDir): Map<CompanyRef, Map<String, Map<String, List<ReportMetadata>>>> {
//    val cikToCompanyName:  MutableMap<String, String> = linkedMapOf()
    return Files.newDirectoryStream(reportIndexLocation)
        .filter { Files.isDirectory(it) }
        .sortedBy { it.fileName.toString().toInt() }
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
                            val companyName = sanitiseCompanyName(it.group(2))
                            if (companyName.toLowerCase().contains("connectivity")) {
                                println(companyName)
                            }
//                            val name = cikToCompanyName.put(cik, )
                            ReportMetadata(
                                year = yearIndexDirectory.fileName.toString(),
                                quarter = quarterDirectory.fileName.toString(),
                                companyRef = CompanyRef(cik, companyName),
                                reportType = it.group(3),
                                date = LocalDate.parse(it.group(4)).atStartOfDay(),
                                reportPath = it.group(5)
                            )
                        }.collect(Collectors.toList())
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

fun sanitiseCompanyName(it: String) =
    it.replace(Regex("[\\.\\,\\'\\/]"), "")
        .replace(" ", "_")
        .toLowerCase()
