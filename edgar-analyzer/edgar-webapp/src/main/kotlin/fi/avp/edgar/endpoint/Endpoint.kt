package fi.avp.edgar.endpoint

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import fi.avp.edgar.*
import fi.avp.util.asyncGetText
import fi.avp.util.mapAsync
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.bson.conversions.Bson
import org.litote.kmongo.*
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.converter.HttpMessageNotReadableException
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.*
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.reactive.CorsWebFilter
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
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
    val interactiveData = fileName?.replace(".txt", "-index.htm")?.let {
        if (!it.contains("www.sec.gov/Archives/")) {
            "https://www.sec.gov/Archives/$it"
        } else it
    } ?: ""

    return FilingDTO(
        ticker ?: "unknown",
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
        liabilities = valueInMillions(liabilities),
        latestAnnualRevenue = valueInMillions(latestRevenue)
    )
}

@Serializable
data class FilingDTO(
    val ticker: String,
    val name: String,
    val reportLink: String,
    val interactiveData: String,
    @Contextual
    val date: LocalDate,
    val type: String,
    val epsYY: Double,
    val eps: Double,
    val revenueYY: Double,
    val revenue: Double,
    val netIncomeYY: Double,
    val netIncome: Double,
    val liabilitiesYY: Double,
    val liabilities: Double,
    val latestAnnualRevenue: Double
)

@Component
class CurrentIndexCrawler {

//    @Scheduled(fixedRate = 24 * 60 * 60 * 1000)
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
//                        .forEach { Database.filings.replaceOne(it) }
                    delay(60000)
                }
        }
    }

    @RestController
    open class Endpoint() {

        private fun filter(revenueThreshold: Double, earliestDate: Long?, company: String): Bson {
            val companyFilter = if (company.isNotBlank()) {
                BasicDBObject("\$text", BasicDBObject("\$search", company))
            } else {
                null
            }

            val dateFilter = earliestDate?.let {
                Filing::dateFiled lt Instant.ofEpochMilli(earliestDate).atZone(ZoneId.systemDefault()).toLocalDate()
            }

            return and(
//                Filing::latestRevenue gt revenueThreshold,
                dateFilter,
                companyFilter
            )
        }



//        @GetMapping(value = ["/filingCount"], produces = [])
//        suspend fun countFilings(@RequestParam until: LocalDate, @RequestParam revenueThreshold: Double, @RequestParam company: String): Long {
//            return Database.filings.countDocuments(filter(revenueThreshold, until, company))
//        }

        @ExceptionHandler
        @ResponseStatus(HttpStatus.BAD_REQUEST)
        open fun handle(e: HttpMessageNotReadableException) {
            e.printStackTrace()
        }

        @GetMapping(value = ["/latestFilings"], produces = [MediaType.APPLICATION_JSON_VALUE])
        suspend fun latestReports(
            @RequestParam limit: Int,
            @RequestParam offset: Int,
            @RequestParam revenueThreshold: Double,
            @RequestParam(required = false) until: Long?,
            @RequestParam(required = false, defaultValue = "") company: String
        ): Flow<FilingDTO> {
            return Database.filings.find(
                filter(
                    revenueThreshold,
                    until,
                    company
                ))
                .sort(BasicDBObject("dateFiled", -1))
                .skip(offset)
                .limit(limit)
                .toFlow().map { it.toDto() }
        }
    }
}
