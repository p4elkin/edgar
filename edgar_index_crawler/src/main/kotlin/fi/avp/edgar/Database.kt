package fi.avp.edgar

import org.litote.kmongo.KMongo
import org.litote.kmongo.find
import org.litote.kmongo.getCollection
import org.litote.kmongo.save
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Database {

    private val client = KMongo.createClient() //get com.mongodb.MongoClient new instance

    val database = client.getDatabase("sec-report") //normal java driver usage
    val reports = database.getCollection("reports", ReportRecord::class.java)
    val reportIndex = database.getCollection("report-index", ReportReference::class.java)
    val xbrl = database.getCollection("xbrl", XBRL::class.java)
    val sp500 = database.getCollection("s-and-p-500")

    val companyList = database.getCollection<CompanyInfo>("company-list")
        .find()
        .batchSize(2000)
        .toList()

    fun getSP500Companies(): List<CompanyInfo> {
        return companyList.filter { it.isInSP500 }
    }

    fun getSP500Tickers(): List<String> {
        return sp500.distinct("ticker", String::class.java).toList()
    }

    fun getTickers(): List<String> {
        return reportIndex.distinct("ticker", String::class.java).toList()
    }

    fun getReportReferencesByCik(cik: String): List<ReportReference> {
        return reportIndex.find("{cik: '$cik'}").toList()
    }

    fun getReportReferencesByTicker(ticker: String): List<ReportReference> {
        return reportIndex.find("{ticker: '$ticker'}").toList()
    }
}

data class XBRL(val reportFileName: String, val xbrl: String?, val cashFlow: String?, val balanceSheet: String?, val incomeStatement: String?)


val filingReferencePattern = Regex("^(.*?)\\s+(10-k|10-Q)\\s+(\\d+)\\s+(\\d+)\\s+(.+.htm)")
fun resolveFilingInfoFromIndexRecord(indexRecord: String): ReportReference? {
    return filingReferencePattern.find(indexRecord)?.let {
        val groups = it.groups
        val dataUrl = groups[5]?.value?.replace("-", "")?.replace("index.htm", "")
        ReportReference(
            ticker = Database.companyList.find { it.cik.contains(groups[3]!!.value.toInt()) }?.primaryTicker ?: "",
            companyName = groups[1]!!.value,
            formType = groups[2]!!.value,
            fileName = groups[5]!!.value,
            dataUrl = dataUrl,
            cik = groups[3]?.value,
            dateFiled = groups[4]?.value?.let {
                LocalDate.parse(
                    it,
                    DateTimeFormatter.ofPattern("yyyyMMdd")
                )
            }
        )
    }
}

data class ReportReference(
    var _id: String? = null,
    val cik: String?,
    val dateFiled: LocalDate?,
    val fileName: String? = null,
    val companyName: String?,
    val formType: String?,
    val revenue: Metric? = null,
    val netIncome: Metric? = null,
    val investingCashFlow: Metric? = null,
    val operatingCashFlow: Metric? = null,
    val financingCashFlow: Metric? = null,
    val fiscalYear: Long? = null,
    val eps: Metric? = null,
    val liabilities: Metric? = null,
    val sharesOutstanding: Metric? = null,
    val assets: Metric? = null,
    val ticker: String? = null,
    var dataUrl: String?,
    val processed: Boolean = false,
    val problems: ReportProblems? = null,
    val contexts: Set<Context>? = emptySet(),
    val units: Set<ValueUnit>? = emptySet(),
    val extractedData: List<ExtractedValue>? = emptyList(),
    var reference: String? = null, // last segment of data URL
    val reportFiles: ReportFiles? = null) {

    init {
        fileName?.let {
            if (dataUrl == null) {
                dataUrl = "$EDGAR_DATA${cik}/${it.replace("-", "")}"
            }
            reference = dataUrl?.substringAfterLast("/")
            _id = reference
        }
    }
}

