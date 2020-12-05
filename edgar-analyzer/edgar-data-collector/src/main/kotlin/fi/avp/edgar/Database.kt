package fi.avp.edgar

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.client.model.IndexOptions
import fi.avp.edgar.util.mapAsync
import fi.avp.edgar.util.runOnComputationThreadPool
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.serialization.Serializable
import me.moallemi.tools.daterange.localdate.LocalDateRange
import org.litote.kmongo.coroutine.coroutine
import org.litote.kmongo.reactivestreams.KMongo
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import me.moallemi.tools.daterange.localdate.rangeTo
import okhttp3.internal.wait
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

        override fun iterator(): Iterator<LocalDateRange> = mapped.reversed().iterator()
    }

    private val connectionString = System.getenv("address") ?: "mongodb://localhost"
    private val asyncClient = KMongo.createClient(connectionString).coroutine

    val database = asyncClient.getDatabase("sec-report") //normal java driver usage
    val filings = database.getCollection<Filing>("filings")
    val cashflow = database.getCollection<CondensedReport>("cashflow")
    val balance = database.getCollection<CondensedReport>("balance")
    val operations = database.getCollection<CondensedReport>("operations")
    val income = database.getCollection<CondensedReport>("income")

    init {
        GlobalScope.async {
            filings.ensureIndex("sic")
        }
    }

    val industryCodes = GlobalScope.async {  database.getCollection<DBObject>("sic")
        .find()
        .batchSize(2000)
        .toList()
        .map {
            it.get("code").toString().toInt() to it.get("description").toString()
        }.toMap()
    }

    private val companyList = GlobalScope.async {  database.getCollection<CompanyInfo>("company-list")
        .find()
        .batchSize(2000)
        .toList()
    }

    suspend fun getAllFilings(): List<CoroutineFindPublisher<Filing>>  {
        return getAllFilings(bsonFilter = null)
    }

    suspend fun getAllFilings(bsonFilter: Bson? = null, sortCriteria: Bson? = null): List<CoroutineFindPublisher<Filing>>  {
        val dataRanges = DateRange()
            .reversed()
            .chunked(4)
        return dataRanges
            .map { ranges ->
                runOnComputationThreadPool {
                    val result = ranges.mapAsync { dateRange ->
                        filings.find(
                            and(
                                Filing::dateFiled gt dateRange.start,
                                Filing::dateFiled lte dateRange.endInclusive,
                                bsonFilter)).sort(sortCriteria ?: BasicDBObject())
                            .noCursorTimeout(true)
//                                .projection(fields(exclude(Filing::extractedData)))

                    }.awaitAll()

//                    val rangeStr = ranges.map { "[${it.start} - ${it.endInclusive}]" }.joinToString()
                    result
                }
            }
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

    suspend fun getAllFilings(filter: String? = null, sortCriteria: String? = null): List<CoroutineFindPublisher<Filing>>  {
        return getAllFilings(filter?.let {KMongoUtil.toBson(it)}, sortCriteria?.let {KMongoUtil.toBson(it)})
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

    suspend fun tryResolveExisting(source: Filing): Filing {
        return filings.findOne(and(Filing::dataUrl eq source.dataUrl, Filing::dateFiled eq source.dateFiled)) ?: source
    }
}

