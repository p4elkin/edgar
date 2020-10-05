package fi.avp.edgar.endpoint

import com.mongodb.BasicDBObject
import fi.avp.edgar.*
import fi.avp.edgar.util.asyncGetText
import fi.avp.edgar.util.forEachAsync
import fi.avp.edgar.util.mapAsync
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
import org.litote.kmongo.coroutine.replaceOne
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration
import org.springframework.boot.autoconfigure.mongo.MongoReactiveAutoConfiguration
import org.springframework.boot.builder.SpringApplicationBuilder
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
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.util.concurrent.Executors


@SpringBootApplication(exclude = [MongoReactiveAutoConfiguration::class, MongoAutoConfiguration::class])
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

    @Scheduled(fixedRate = 24 * 60 * 60 * 1000)
    fun crawl() {
        val daysBack: Long = 40
        runBlocking(Executors.newFixedThreadPool(8).asCoroutineDispatcher()) {
            val newFilings = getFilingsAfter(LocalDate.now().minusDays(daysBack))
                    .flatMap {
                        asyncGetText(it.url)
                                .split("\n")
                                .map { it }
                                .mapNotNull { resolveFilingInfoFromIndexRecord(it) }
//                                .filter { Database.tryResolveExisting(it) != it }
                    }
                    // process filings in batches by five


            println("About to parse ${newFilings.size} new filings")
            newFilings
                    .chunked(5)
                    .forEach {
                        println("Will now parse $it")
                        // resolve everything up to the year to year changes
                        it.mapAsync { scrapeFilingFacts(it) }
                                .awaitAll()
                                .forEachAsync {
                                    if (it._id != null) {
                                        Database.filings.replaceOne(it)
                                    } else {
                                        Database.filings.save(it)
                                    }
                                }

                        delay(5000)
                    }
        }
    }

    data class Filter(
        val startDate: Long?,
        val endDate: Long?,
        val company: String?
    )

    @RestController
    open class Endpoint() {

        private fun localDateFromMillis(millis: Long) =
            Instant.ofEpochMilli(millis).atZone(ZoneId.systemDefault()).toLocalDate()


        private fun filter(filter: Filter): Bson {
            val companyFilter = filter.company?.let {
                BasicDBObject("\$text", BasicDBObject("\$search", it))
            }

            val dateFilter = and(
                Filing::dateFiled gte (filter.startDate?.let { localDateFromMillis(it)} ?: LocalDate.of(2011, 1, 1)),
                Filing::dateFiled lte (filter.endDate?.let { localDateFromMillis(it)} ?: LocalDate.now()))


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

        @GetMapping(value = ["/latestFilings"], produces = [MediaType.APPLICATION_JSON_VALUE])
        suspend fun latestFilings(@RequestParam limit: Int, @RequestParam offset: Int, filter: Filter): Flow<FilingDTO> {
            return Database.filings.find(filter(filter))
                .sort(BasicDBObject("dateFiled", -1))
                .skip(offset)
                .limit(limit)
                .toFlow().map { it.toDto() }
        }
    }
}
