package fi.avp.edgar

import com.mongodb.client.MongoCollection
import org.litote.kmongo.aggregate

data class AggregatedReport(val _id: String, val series: List<ReportSeries>)
data class CompanyReports(val ticker: String?, val reports: List<AggregatedReport>)

fun MongoCollection<ReportRecord>.getReportsOfType(ticker: String = "", reportType: String): List<CompanyReports> {
    return aggregate<CompanyReports>(
        matchTicker(ticker),
        filterByReportType(reportType)
    ).toList()
}

private fun filterByReportType(reportType: String) =
    "{\$project: {ticker: 1, reports: {\$filter: {input: '\$reports', as: 'rep', cond: {\$eq: ['\$\$rep._id', '$reportType']}}}}}"

private fun matchTicker(ticker: String) =
    if (ticker.isBlank()) "{\$match: {}}" else "{\$match: {ticker: '$ticker'}}"

