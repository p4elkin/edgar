package fi.avp.edgar

import com.mongodb.BasicDBObject
import fi.avp.edgar.util.mapAsync
import fi.avp.edgar.util.runOnComputationThreadPool
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.coroutineScope
import kotlinx.serialization.Serializable
import me.moallemi.tools.daterange.localdate.LocalDateRange
import org.litote.kmongo.coroutine.coroutine
import org.litote.kmongo.reactivestreams.KMongo
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import me.moallemi.tools.daterange.localdate.rangeTo
import org.bson.conversions.Bson
import org.litote.kmongo.*
import org.litote.kmongo.coroutine.CoroutineFindPublisher
import org.litote.kmongo.util.KMongoUtil
import java.time.temporal.ChronoUnit



@Serializable
data class CompanyInfo(
    val _id: String? = null,
    val primaryTicker: String,
    val tickers: Set<String> = emptySet(),
    val isInSP500: Boolean = false,
    val cik: Set<Int>,
    val names: Set<String>,
    val description: String = "",
    val exchange: String = "",
    val sic: Int = -1)

object Database {

    class DateRange(val startYear: Int = 2009, val endYearInclusive: Int = 2020, val chunks: Int = 64): Iterable<LocalDateRange> {

        val startDate: LocalDate = LocalDate.of(startYear, 1, 1)
        val endDate: LocalDate = LocalDate.of(endYearInclusive + 1, 1, 1)
        val steps: Long = (ChronoUnit.DAYS.between(startDate, endDate) / chunks).toLong()
        val range = (startDate..endDate step steps).toList()

        val mapped = range.take(range.size - 1).mapIndexed { index, value ->
            value..range[index + 1]
        }

        override fun iterator(): Iterator<LocalDateRange> = mapped.iterator()
    }

    private val asyncClient = KMongo.createClient().coroutine

    val database = asyncClient.getDatabase("sec-report") //normal java driver usage
    val filings = database.getCollection<Filing>("filings")
    val cashflow = database.getCollection<CondensedReport>("cashflow")

    private val companyList = GlobalScope.async {  database.getCollection<CompanyInfo>("company-list")
        .find()
        .batchSize(2000)
        .toList()
    }

    suspend fun getAllFilings(): List<CoroutineFindPublisher<Filing>>  {
        return getAllFilings(bsonFilter = null)
    }

    suspend fun getAllFilings(bsonFilter: Bson? = null): List<CoroutineFindPublisher<Filing>>  {
        return DateRange()
            .reversed()
            .chunked(4)
            .mapAsync {
                runOnComputationThreadPool {
                    it.map {
                        val result =
                            filings.find(
                                and(
                                    Filing::dateFiled gt it.start,
                                    Filing::dateFiled lte it.endInclusive,
                                    bsonFilter))
//                                .projection(fields(exclude(Filing::extractedData)))
                        result
                    }
                }
            }.awaitAll()
            .flatten()
    }

    data class LiveCompanies(
        val cik: List<Long>,
        val tickers: List<String>,
        val names: List<String>) {
        fun contain(filing: Filing): Boolean {
            return cik.contains(filing.cik) || tickers.contains(filing.ticker) || names.contains(filing.companyName)
        }
    }

    suspend fun getLiveCompanies() = coroutineScope {
        val cik = async { getExistingCompanyCikList() }
        val ticker = async { getExistingCompanyTickerList() }
        val names = async { getExistingCompanyNameList() }

        LiveCompanies(
            cik = cik.await(),
            tickers = ticker.await(),
            names = names.await())
    }

    suspend fun getExistingCompanyCikList(): List<Long> {
        return filings.distinct<Long>("cik", "{formType: '10-K', fiscalYear: 2019}").toList()
    }

    suspend fun getExistingCompanyTickerList(): List<String> {
        return filings.distinct<String>("ticker", "{formType: '10-K', fiscalYear: 2019}").toList()
    }

    suspend fun getExistingCompanyNameList(): List<String> {
        return filings.distinct<String>("ticker", "{formType: '10-K', fiscalYear: 2019}").toList()
    }

    suspend fun getAllFilings(filter: String? = null): List<CoroutineFindPublisher<Filing>>  {
        return getAllFilings(filter?.let {KMongoUtil.toBson(it)})
    }

    suspend fun getCompanyList(): List<CompanyInfo> {
        return companyList.await()
    }

    suspend fun getSP500Companies(): List<CompanyInfo> {
        return getCompanyList().filter { it.isInSP500 }
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

val filingReferencePattern = Regex("^(.*?)\\s+(10-k|10-Q)\\s+(\\d+)\\s+(\\d+)\\s+(.+.htm)")
suspend fun resolveFilingInfoFromIndexRecord(indexRecord: String): Filing? {
    return filingReferencePattern.find(indexRecord)?.let {
        val groups = it.groups
        val dataUrl = groups[5]?.value?.replace("-", "")?.replace("index.htm", "")
        Filing(
            ticker = Database.getCompanyList().find { it.cik.contains(groups[3]!!.value.toInt()) }?.primaryTicker ?: "",
            companyName = groups[1]!!.value,
            formType = groups[2]!!.value,
            fileName = groups[5]!!.value,
            dataUrl = dataUrl,
            cik = groups[3]?.value?.toLong(),
            dateFiled = groups[4]?.value?.let {
                LocalDate.parse(
                    it,
                    DateTimeFormatter.ofPattern("yyyyMMdd")
                )
            }
        )
    }
}

