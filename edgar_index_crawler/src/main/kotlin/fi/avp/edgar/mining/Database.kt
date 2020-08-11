package fi.avp.edgar.mining

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import fi.avp.edgar.CompanyInfo
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.ContextualSerialization
import kotlinx.serialization.Serializable
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.litote.kmongo.coroutine.*
import org.litote.kmongo.gt
import org.litote.kmongo.reactivestreams.KMongo

object Database {

    private val asyncClient = KMongo.createClient().coroutine

    val database = asyncClient.getDatabase("sec-report") //normal java driver usage
    val filings = database.getCollection<Filing>("report-index")
    val sp500 = database.getCollection<DBObject>("s-and-p-500")

    val companyList = runBlocking {  database.getCollection<CompanyInfo>("company-list")
        .find()
        .batchSize(2000)
        .toList()
    }

    fun getSP500Companies(): List<CompanyInfo> {
        return companyList.filter { it.isInSP500 }
    }

    suspend fun getSP500Tickers(): List<String> {
        return sp500.distinct<String>("ticker", "{}").toList()
    }

    suspend fun getTickers(): List<String> {
        return filings.distinct<String>("ticker", "{}").toList()
    }

    suspend fun getFilingsByCik(cik: String): List<Filing> {
        return try {
            filings.find("{cik: '$cik'}").toList()
        } catch (e: Exception) {
            return emptyList()
        }
    }

    suspend fun getFilingsByTicker(ticker: String): List<Filing> {
        return filings.find("{ticker: '$ticker'}").toList()
    }

    suspend fun getLatestFilings(days: Int): List<Filing> {
        val sortCriteria = BasicDBObject("dateFiled", -1)
        return filings
            .find(Filing::dateFiled gt LocalDate.now().minusDays(days.toLong()))
            .sort(sortCriteria)
            .toList()
    }

    suspend fun tryResolveExisting(stub: Filing): Filing {
        return filings.findOne("{dataUrl: '${stub.dataUrl}'}") ?: stub
    }
}

data class XBRL(val reportFileName: String, val xbrl: String?, val cashFlow: String?, val balanceSheet: String?, val incomeStatement: String?)

val filingReferencePattern = Regex("^(.*?)\\s+(10-k|10-Q)\\s+(\\d+)\\s+(\\d+)\\s+(.+.htm)")
fun resolveFilingInfoFromIndexRecord(indexRecord: String): Filing? {
    return filingReferencePattern.find(indexRecord)?.let {
        val groups = it.groups
        val dataUrl = groups[5]?.value?.replace("-", "")?.replace("index.htm", "")
        Filing(
            ticker = Database.companyList.find { it.cik.contains(groups[3]!!.value.toInt()) }?.primaryTicker
                ?: "",
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

enum class OperationStatus {
    DONE,
    FAILED,
    PENDING
}

@Serializable
data class Filing(
    var _id: String? = null,
    val cik: String?,
    @ContextualSerialization
    val dateFiled: LocalDate?,
    val fileName: String? = null,
    val companyName: String?,
    val formType: String?,
    val revenue: Metric? = null,
    @ContextualSerialization
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
    val files: ReportFiles? = null,
    val closestYearReportId: String? = null,
    val latestRevenue: Double? = null,
    val dataExtractionStatus: OperationStatus? = OperationStatus.PENDING,
    val yearToYearUpdate: OperationStatus? = OperationStatus.PENDING) {

    init {
        fileName?.let {
            if (dataUrl == null) {
                dataUrl = "$EDGAR_DATA${cik}/${it.replace("-", "")}"
            }
            reference = dataUrl?.substringAfterLast("/")
        }
    }
}

