package fi.avp.edgar.endpoint

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
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.reactive.CorsWebFilter
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource
import reactor.core.publisher.Flux
import reactor.core.publisher.toFlux
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


@Component
class CurrentIndexCrawler {

//    @Scheduled(fixedRate = 24 * 60 * 60 * 1000)
    fun crawl() {
        runBlocking(Executors.newFixedThreadPool(8).asCoroutineDispatcher()) {
            val filings = getFilingsAfter(LocalDate.now().minusDays(1))
                .flatMap {
                    asyncGetText(it.url)
                        .split("\n")
                        .mapNotNull { resolveFilingInfoFromIndexRecord(it) }
                        .filter { Database.filings.findOne("{dataUrl: '${it.dataUrl}'}") == null }
                }

            filings.filter { !it.processed }.chunked(2).forEach {
                it.mapAsync { resolveYearToYearChanges(it) }
                    .awaitAll()
                    .forEach { Database.filings.save(it) }
                delay(60000)
            }
        }
    }

    @RestController
    open class Endpoint() {

        @GetMapping(value = ["/latestFilings"], produces = [MediaType.APPLICATION_JSON_VALUE])
        fun latestReports(): List<FilingDTO> {
            val latestFilings = Database.getLatestFilings(10)

            return latestFilings
                .filter {
                    it.assets?.value?.let { it > 1_000_000_000} ?: false
                }
                .map {
                    val reportLink = it.reportFiles?.visualReport ?: ""
                    val interactiveData = it.fileName?.replace(".txt", "-index.htm") ?: ""
                    FilingDTO(
                        it.ticker!!,
                        it.companyName!!,
                        reportLink,
                        interactiveData = interactiveData,
                        date = it.dateFiled!!,
                        type = it.formType!!,
                        epsYY = it.eps?.relativeYearToYearChange() ?: Double.NaN,
                        eps = it.eps?.value ?: Double.NaN,

                        revenueYY = it.revenue?.relativeYearToYearChange() ?: Double.NaN,
                        revenue = it.revenue?.value ?: Double.NaN,

                        netIncomeYY = it.netIncome?.relativeYearToYearChange() ?: Double.NaN,
                        netIncome = it.netIncome?.value ?: Double.NaN,

                        liabilitiesYY = it.liabilities?.relativeYearToYearChange() ?: Double.NaN,
                        liabilities = it.liabilities?.value ?: Double.NaN
                    )
                }
        }
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
}
