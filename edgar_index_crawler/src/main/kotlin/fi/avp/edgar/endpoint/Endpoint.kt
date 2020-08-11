package fi.avp.edgar.endpoint

import com.mongodb.BasicDBObject
import fi.avp.edgar.mining.*
import fi.avp.util.asyncGetText
import fi.avp.util.mapAsync
import kotlinx.coroutines.*
import org.litote.kmongo.*
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.http.MediaType
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.reactive.CorsWebFilter
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource
import java.time.LocalDate
import java.util.concurrent.Executors

@SpringBootApplication
@EnableScheduling
open class SecReportDataApplication {

    @Bean
    open fun corsFilter(): CorsWebFilter = CorsWebFilter(
        UrlBasedCorsConfigurationSource().apply {
            registerCorsConfiguration(
                "/**",
                CorsConfiguration().apply {
                    allowCredentials = true
                    allowedOrigins = listOf("*")
                    allowedHeaders = listOf("*")
                    allowedMethods = listOf("*")
                }
            )
        })
}

fun main(args: Array<String>) {
    runApplication<SecReportDataApplication>(*args)
}

fun Filing.toDto(): FilingDTO {
    val reportLink = files?.visualReport ?: ""
    val interactiveData = fileName?.replace(".txt", "-index.htm") ?: ""
    return FilingDTO(
        ticker!!,
        companyName!!,
        reportLink,
        interactiveData = interactiveData,
        date = dateFiled!!,
        type = formType!!,
        epsYY = eps?.relativeYearToYearChange() ?: Double.NaN,
        eps = eps?.value ?: Double.NaN,

        revenueYY = revenue?.relativeYearToYearChange() ?: Double.NaN,
        revenue = valueInMillions(revenue),

        netIncomeYY = netIncome?.relativeYearToYearChange() ?: Double.NaN,
        netIncome = valueInMillions(netIncome),

        liabilitiesYY = liabilities?.relativeYearToYearChange() ?: Double.NaN,
        liabilities = valueInMillions(liabilities))

}

data class FilingDTO(
    val ticker: String,
    val name: String,
    val reportLink: String,
    val interactiveData: String,
    val date: LocalDate,
    val type: String,
    val epsYY: Double,
    val eps: Double,
    val revenueYY: Double,
    val revenue: Double,
    val netIncomeYY: Double,
    val netIncome: Double,
    val liabilitiesYY: Double,
    val liabilities: Double
)

@Component
class CurrentIndexCrawler {

    @Scheduled(fixedRate = 24 * 60 * 60 * 1000)
    fun crawl() {
        runBlocking(Executors.newFixedThreadPool(8).asCoroutineDispatcher()) {
            getFilingsAfter(LocalDate.now().minusDays(1))
                .flatMap {
                    asyncGetText(it.url)
                        .split("\n")
                        .mapNotNull { resolveFilingInfoFromIndexRecord(it) }
                        .map { Database.tryResolveExisting(it) }
                }
                // process filings in batches by two
                .chunked(5)
                .forEach {
                    // resolve everything up to the year to year changes
                    it.mapAsync { scrapeFilingFacts(it) }
                        .awaitAll()
                        .forEach { Database.filings.save(it) }
                    delay(60000)
                }
        }
    }

    class Filings(val filings: List<Filing>) {

        val relatedAnnualReports: Map<Filing, Filing>
        val filingById: Map<String, Filing>

        init {
            filingById = filings.map { it._id!! to it }.toMap()
            relatedAnnualReports = filings
                .filter { it.closestYearReportId != null }
                .map { it to filingById[it.closestYearReportId]!! }
                .toMap()
        }
    }

    @RestController
    open class Endpoint() {

//        private val filings = runBlocking {
//            Database.filings.find(Filing::dateFiled gt LocalDate.now().minusDays(2000)).toList()
//        }

        private val cache = runBlocking {
            Database.filings.find(Filing::dateFiled gt LocalDate.now().minusDays(100)).toList()
        }

        @GetMapping(value = ["/filingCount"], produces = [])
        fun countFilings(@RequestParam dayOffset: Int, @RequestParam revenueThreshold: Int): Int {
            val earliestDate = LocalDate.now().minusDays(dayOffset.toLong())
            return cache.filter {
                it.assets?.value?.let { it > revenueThreshold} ?: false &&
                it.dateFiled!!.isAfter(earliestDate)
            }.count()
        }

        @GetMapping(value = ["/latestFilings"], produces = [MediaType.APPLICATION_JSON_VALUE])
        fun latestReports(@RequestParam limit: Int, @RequestParam offset: Int, @RequestParam revenueThreshold: Long = 1000_000_000, @RequestParam dayOffset: Int = 5): List<FilingDTO> {
            val earliestDate = LocalDate.now().minusDays(dayOffset.toLong())
            return cache
                .filter {
                    val isEarningEnough = it.assets?.value?.let { it > revenueThreshold} ?: false
                    isEarningEnough && it.dateFiled!!.isAfter(earliestDate)
                }
                .drop(offset)
                .take(limit)
                .map { it.toDto() }
        }
    }
}
