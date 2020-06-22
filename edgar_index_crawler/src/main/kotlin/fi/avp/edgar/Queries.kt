package fi.avp.edgar

import com.mongodb.client.MongoCollection
import org.litote.kmongo.aggregate

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

