package fi.avp.edgar

import org.litote.kmongo.KMongo
import org.litote.kmongo.find
import org.litote.kmongo.save
import java.time.LocalDate
import javax.print.attribute.standard.JobKOctetsProcessed

object Database {
    val client = KMongo.createClient() //get com.mongodb.MongoClient new instance
    val database = client.getDatabase("sec-report") //normal java driver usage
    val reports = database.getCollection("reports", ReportRecord::class.java)
    val reportIndex = database.getCollection("report-index", ReportReference::class.java)
    val xbrl = database.getCollection("xbrl", XBRL::class.java)
    val tickers = database.getCollection("ticker", TickerMapping::class.java)
    val sp500 = database.getCollection("s-and-p-500")

    fun getSP500Tickers(): List<String> {
        return sp500.distinct("ticker", String::class.java).toList()
    }

    fun getTickers(): List<String> {
        return reportIndex.distinct("ticker", String::class.java).toList()
    }

    fun getReportReferences(ticker: String): List<ReportReference> {
        return reportIndex.find("{ticker: '$ticker'}").toList()
    }

    fun storeExtractedData(ref: ReportReference, data: ReportDataExtractionResult) {
        val relatedContexts = data.data.flatMap { it.contexts }.toSet()
        val relatedUnits = data.data.flatMap { it.valueUnits ?: emptySet() }.toSet()

        reportIndex.save(ref.copy(
            processed = true,
            contexts = relatedContexts,
            units = relatedUnits,
            problems = if (data.problems.missingProperties.isEmpty() && data.problems.suspiciousContexts.isEmpty()) null else data.problems,
            extractedData = data.data.flatMap { it.extractedValues
                .filter { it.unit != null || it.propertyId.startsWith("dei:") }
                .filter { it.value.length <= 100 }
            }))
    }
}

data class TickerMapping(val _id: String, val ticker: String, val cik: String)
data class XBRL(val dataUrl: String, val xbrl: String?)

data class ReportReference(
    val _id: String,
    val cik: String?,
    val revenue: Metric?,
    val netIncome: Metric?,
    val investingCashFlow: Metric?,
    val operatingCashFlow: Metric?,
    val financingCashFlow: Metric?,
    val fiscalYear: Long?,
    val eps: Metric?,
    val liabilities: Metric?,
    val sharesOutstanding: Metric?,
    val assets: Metric?,
    val dateFiled: LocalDate?,
    val fileName: String?,
    val companyName: String?,
    val formType: String?,
    val reportFile: String?,
    val ticker: String?,
    var dataUrl: String?,
    val processed: Boolean = false,
    val problems: ReportProblems?,
    val contexts: Set<Context>?,
    val units: Set<ValueUnit>?,
    val extractedData: List<ExtractedValue>?,
    var reference: String?, // last segment of data URL
    val reportFiles: ReportFiles?) {

    init {
        val reportId = fileName.let {
            val id = it?.substringAfterLast("/")
            id?.substring(0, id.length - 4)
        }

        dataUrl = "$EDGAR_DATA${cik}/${reportId?.replace("-", "")}"
        reference = dataUrl?.substringAfterLast("/")
    }
}

