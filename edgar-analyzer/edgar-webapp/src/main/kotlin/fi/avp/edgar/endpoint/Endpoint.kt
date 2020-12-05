package fi.avp.edgar.endpoint

import com.mongodb.BasicDBObject
import fi.avp.edgar.CondensedReport
import fi.avp.edgar.Database
import fi.avp.edgar.endpoint.dto.FilingDTO
import fi.avp.edgar.endpoint.dto.Filter
import fi.avp.edgar.endpoint.dto.toDto
import fi.avp.edgar.util.Locations
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration
import org.springframework.boot.autoconfigure.mongo.MongoReactiveAutoConfiguration
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.http.MediaType.*
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.reactive.CorsWebFilter
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource
import reactor.core.publisher.Mono
import java.nio.charset.StandardCharsets
import java.nio.file.Files

@SpringBootApplication(exclude = [MongoReactiveAutoConfiguration::class, MongoAutoConfiguration::class])
open class SecReportDataApplication {

    @Bean
    open fun corsFilter(): CorsWebFilter = CorsWebFilter(
        UrlBasedCorsConfigurationSource().apply {
            registerCorsConfiguration(
                "/**",
                CorsConfiguration().apply {
                    allowCredentials = true
                    allowedOriginPatterns = listOf("*")
                    allowedHeaders = listOf("*")
                    allowedMethods = listOf("*")
                }
            )
        })

}

fun main(args: Array<String>) {
    runApplication<SecReportDataApplication>(*args)
}

@RestController
open class Endpoint() {

    @GetMapping(value = ["/tickers"], produces = ["application/json"])
    open suspend fun tickers(): List<String> {
        return Database.getCompanyList().flatMap {
            it.tickers
        }
    }

    @GetMapping(value = ["/quotes"], produces = ["application/json"])
    open suspend fun quotes(@RequestParam ticker: String): String {
        val stockData = Locations.splitData.resolve("$ticker.json")
        if (!Files.exists(stockData)) {
            return "[]";
        }
        return stockData.toFile().readText(StandardCharsets.UTF_8)
    }


    @GetMapping(value = ["/10k"], produces = [APPLICATION_JSON_VALUE])
    open suspend fun metricSeries(@RequestParam ticker: String): Flow<FilingDTO> {
        return Database.filings.find("{ticker: '$ticker', formType: '10-K'}, {extractedData: -1}")
            .sort(BasicDBObject("dateFiled", 1))
            .toFlow()
            .filter {
                it.fiscalYear != null
            }
            .map {
                it.toDto()
            }
    }

    @GetMapping(value = ["/filing"], produces = [APPLICATION_JSON_VALUE])
    open suspend fun filingInfoById(@RequestParam id: String): FilingDTO? {
        return Database.filings
                .findOne("{_id: ObjectId('$id')}")?.toDto()
    }

    @GetMapping(value = ["/cik"], produces = [APPLICATION_JSON_VALUE])
    suspend fun industryCodes(@RequestParam id: String, @RequestParam type: String): Map<Int, String> {
        return Database.industryCodes.await()
    }

    @GetMapping(value = ["/condensedReports"], produces = [APPLICATION_JSON_VALUE])
    suspend fun condensedReports(@RequestParam id: String, @RequestParam type: String): CondensedReport? {
        return Database.filings
                .findOne("{_id: ObjectId('$id')}")?.let {filing ->
                    val (collection, fileName) = when(type) {
                        "balance" -> Database.balance to filing.files?.balance
                        "income" -> Database.income to filing.files?.income
                        "cashflow" -> Database.cashflow to filing.files?.cashFlow
                        "operations" -> Database.operations to filing.files?.operations
                        else -> throw RuntimeException("Unknown condensed report statement $type")
                    }

                    collection.findOne("{dataUrl: '${filing.dataUrl}/$fileName'}")
                }
    }

    @GetMapping(value = ["/latestFilings"], produces = [APPLICATION_JSON_VALUE])
    open fun latestFilings(@RequestParam limit: Int, @RequestParam offset: Int, filter: Filter): Flow<FilingDTO> {
        return Database.filings.find(filter.toBson())
                .sort(BasicDBObject("dateFiled", -1))
                .skip(offset)
                .limit(limit)
                .toFlow().map { it.toDto() }
    }
}
