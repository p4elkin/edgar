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
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.reactive.CorsWebFilter
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource
import reactor.core.publisher.Flux
import reactor.core.publisher.toFlux
import java.nio.charset.StandardCharsets
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

public suspend fun collectFilingData(it: ReportReference): ReportReference {
    val filingReference = it.copy(reportFiles = fetchRelevantFileNames(it))
    delay(10000)

    val data = downloadSingleReport(filingReference)
    return data?.xbrl?.let {
        parseReport(
            it.byteInputStream(StandardCharsets.UTF_8),
            filingReference
        )
    } ?: filingReference
}

suspend fun getFilingInformation(ref: ReportReference): Deferred<ReportReference> {
    return if (ref.processed)
        CompletableDeferred(ref)
    else coroutineScope {
        async {
            collectFilingData(ref)
        }
    }
}

@Component
class CurrentIndexCrawler {

    @Scheduled(fixedRate = 24 * 60 * 60 * 1000)
    fun crawl() {
        runBlocking(Executors.newFixedThreadPool(8).asCoroutineDispatcher()) {
            val filings = getFilingsAfter(LocalDate.now().minusDays(5))
                .flatMap {
                    asyncGetText(it.url)
                        .split("\n")
                        .mapNotNull { resolveFilingInfoFromIndexRecord(it) }
                        .filter { Database.reportIndex.findOne("{dataUrl: '${it.dataUrl}'}") == null }
                }

            filings.chunked(2).forEach {
                it.mapAsync { collectFilingData(it) }
                    .awaitAll()
                    .forEach { Database.reportIndex.save(it) }
                delay(60000)
            }
        }
    }

    @RestController
    open class Endpoint() {

        val collection = Database.database.getCollection("annual", ReportRecord::class.java)

        @GetMapping(value = ["/{ticker}/{reportType}"], produces = [MediaType.APPLICATION_JSON_VALUE])
        fun prices(
            @PathVariable(value = "ticker") ticker: String,
            @PathVariable(value = "reportType") reportType: String
        ): Flux<CompanyReports> {
            val aggregationResult = collection.aggregate<CompanyReports>(
                "{\$match: {ticker: '$ticker'}}",
                "{\$project: {ticker: 1, reports: {\$filter: {input: '\$reports', as: 'rep', cond: {\$eq: ['\$\$rep._id', '$reportType']}}}}}"
            ).toList()

            return aggregationResult.toFlux()
        }

        @GetMapping(value = ["/latestFilings"], produces = [MediaType.APPLICATION_JSON_VALUE])
        fun latestReports(): List<FilingDTO> {
            val sortCriteria = BasicDBObject("dateFiled", -1)
            val latestFilings = Database.reportIndex
                .find(ReportReference::dateFiled gt LocalDate.now().minusDays(10))
                .sort(sortCriteria)
                .limit(100)
                .toList()

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
                        type = it.formType!!
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
        val type: String
    )
}
