package fi.avp.edgar

import fi.avp.edgar.data.CompanyRef
import fi.avp.edgar.data.ReportMetadata
import fi.avp.util.companyQuarterlyReport
import fi.avp.util.preparePerCompanyReportStorageStructure
import java.nio.file.Path
import java.time.ZoneOffset

val reports = preparePerCompanyReportStorageStructure();

fun getCompanyNames(): List<String> {
    return reports.keys.map { it.name }
}

fun streamCompanyReports(company: String): List<Pair<ReportMetadata, Path>> {
    return streamCompanyReports { it.name == company }
}
/**
 * Stream available report files mapped to the relevant metadata.
 */
fun streamCompanyReports(companyFilter: (CompanyRef) -> Boolean = { true }): List<Pair<ReportMetadata, Path>> {
    return reports
        .filterKeys(companyFilter)
        .flatMap { (_, annualData) ->
            annualData.flatMap { (year, reports)  ->
                reports.values.flatten()
                    .sortedBy { it.date.toInstant(ZoneOffset.UTC).toEpochMilli() }
            }
        }.mapNotNull {
                reportRecord -> companyQuarterlyReport(reportRecord)?.let { reportRecord to it }
        }
}
