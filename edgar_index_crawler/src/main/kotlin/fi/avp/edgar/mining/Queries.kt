package fi.avp.edgar.mining

import com.mongodb.client.MongoCollection
import org.litote.kmongo.aggregate

data class AggregatedReport(val _id: String, val series: List<ReportSeries>)
data class CompanyReports(val ticker: String?, val reports: List<AggregatedReport>)


