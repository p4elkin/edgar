package fi.avp.edgar.mining

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import fi.avp.edgar.data.attrNames
import fi.avp.util.mapAsync
import kotlinx.coroutines.awaitAll
import org.bson.Document
import org.litote.kmongo.aggregate
import java.time.LocalDate

val company = Database.database.getCollection("companies")

fun main() {
//    quarterlyReportSeries.drop()
//    annualReportSeries.drop()
//    runBlocking {
//        getCompanyNames().map { it.replace("/", "//") }.forEach {
//            quarterlyReportSeries.insertOne(computeReports(it, false))
//            annualReportSeries.insertOne(computeReports(it, true))
//        }
//    }
}

private suspend fun computeReports(companyName: String, isAnnual: Boolean): Document {
    val companyReports = Document()
        .append("name", companyName)
        .append("ticker", getTicker(companyName))

    val reportType = if (isAnnual) "10-K" else "10-Q"
    val aggregatedReports = attrNames.mapAsync { propertyDescriptor ->
        propertyDescriptor.id to propertyDescriptor.variants.mapAsync { variant ->
            generateReport(
                companyName,
                variant,
                reportType,
                propertyDescriptor.id
            )
        }.awaitAll()
    }.awaitAll().map { reports ->
        val categoryDoc = Document("_id", reports.first)

        val reportSeries = reports.second
        categoryDoc.append("series", reportSeries.filter { it?.metrics?.any { it.value != null } ?: false })

        val hasCompleteReport = reportSeries.any {
            it?.metrics?.all { it.value != null } ?: false
        }

        val isEmpty = reportSeries.none {
            it?.metrics?.any { it.value != null } ?: false
        }

        categoryDoc
            .append("hasCompleteReport", hasCompleteReport)
            .append("isEmpty", isEmpty)
    }

    companyReports.append("reports", aggregatedReports)
    return companyReports
}

data class ReportEntry(
    val reportId: String?,
    val date: LocalDate,
    @JsonDeserialize(using = ValueDeserializer::class)
    val value: String?
)

data class ReportSeries(
    val _id: String,
    val metrics: List<ReportEntry>?
)

fun getTicker(companyName: String): String? {
    TODO("implement via database")
//    return try {
//        val ticker = company.findOne("{cik_str: ${getTrimmedCik(companyName)}}")?.let {
//            it["ticker"] as String
//        }
//        println("$companyName -> $ticker")
//        ticker
//    } catch (e: Exception) {
//        null
//    }
}
//
//fun getTrimmedCik(companyName: String): Int {
//    return reportData.aggregate<DBObject>(
//        "{\$match: {name: '$companyName'}}",
//        "{\$project: {'cik': {\$ltrim: {input: '\$cik', chars: '0'}}}}").first()!!["cik"].toString().toInt()
//}

private fun generateReport(companyName: String, prop: String, reportType: String, category: String): ReportSeries? {
    return try {
        // TODO("adapt to the change of the collection")
        Database.filings.aggregate<ReportSeries>(
            // filter to reports of specific company
            "{\$match: {name: '$companyName', type: '$reportType'}}",
            // include only some props
            "{\$project: {_id: 1, name: 1, date: 1, metrics: 1}}",
            // un-roll all the metrics
            "{\$unwind: {path: '\$metrics'}}",
            // take only the ones belonging to specific category
            "{\$match: {'metrics.type': '$category'}}",
            // aggregate them by date
            "{\$group: {_id: '\$date', metrics: {\$push: {reportId: '\$_id', metric:'\$metrics'}}}}",
            // sort
            "{\$sort: {'_id': 1}}",
            // leave only the ones that are backed by specified property, leave blanks for all the others
            "{\$project: {date: 1, 'val': {\$filter: {input: '\$metrics', as: 'entry', cond: { '\$eq': ['\$\$entry.metric.sourcePropertyName', '$prop']}}}}}",
            "{\$project: {date: 1, 'val': {\$arrayElemAt: ['\$val', 0]}}}",
            // finally group by (for some reason there could be several values backed by same prop, take only the first one)
            "{\$group: {_id: '$prop', metrics: {\$push: {reportId: '\$val.reportId', date: '\$_id', value: '\$val.metric.value'}}}}"
        ).first()
    } catch (e: Exception) {
        println("Failed to parse reports for $companyName due to $e")
        null
    }
}

open class ValueDeserializer: JsonDeserializer<String?>() {
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): String? {
        val strValue: String? = p.readValueAsTree<JsonNode>().asText()
        if (strValue?.endsWith("000000") == false) {
            return strValue.replace(",", "").plus("000000")
        }
        return strValue
    }
}




