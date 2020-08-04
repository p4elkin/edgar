package fi.avp.edgar.mining

import fi.avp.util.mapAsync
import kotlinx.coroutines.*
import org.bson.codecs.pojo.annotations.BsonId
import org.litote.kmongo.save
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.io.InputStream
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.concurrent.Executors
import kotlin.system.measureTimeMillis

data class ReportRecord(
    val cik: String,
    val name: String,
    @BsonId val _id: String,
    val type: String,
    val date: LocalDate,
    val dataUrl: String,
    val reportFileName: String,
    val relatedContexts: List<Context> = emptyList(),
    val extracts: Map<String, String>
)

private val taskDispatcher: CoroutineDispatcher = Executors.newFixedThreadPool(16).asCoroutineDispatcher()

@SpringBootApplication(scanBasePackages = ["fi.avp.edgar.mining"])
@Configuration
open class LargeCapParser {

    @Bean
    open fun commandLineRunner(ctx: ApplicationContext): CommandLineRunner? {
        return CommandLineRunner { args ->
            runBlocking {
                parseLargeCapReports()
            }
        }
    }

}

fun main(args: Array<String>) {
    runApplication<LargeCapParser>(*args)
}

private suspend fun parseLargeCapReports() {
    val sP500Companies = Database.getSP500Companies()
    sP500Companies.forEach {
        val doneIn = measureTimeMillis {
            val reportReferences = it.cik.flatMap {
                Database.getReportReferencesByCik(
                    it.toString()
                )
            }.filter { it.formType == "10-K" }

            println("Parsing data of ${it.tickers} (${reportReferences.size} files)")

            val reportData = getCompanyReports(it.primaryTicker)
            val parsedReports = withContext(taskDispatcher) {
                parseReports(reportReferences, reportData)

            }

            println("saving ${reportReferences.size} reports")
            parsedReports.forEach {
                Database.reportIndex.save(it)
            }
        }
        println("parsed $it in $doneIn ms")
    }
}

suspend fun parseReports(reportReferences: List<ReportReference>, reportData: Map<String, InputStream>): List<ReportReference> {
    return reportReferences
        .mapAsync { reportReference ->
            println("resolving ${reportReference.dataUrl}")
            val data = reportReference.reportFiles?.xbrlReport?.let { reportData[it] }
            if (data == null) {
                println("missing report data for: ${reportReference.reference}")
                reportReference
            } else {
                parseReport(data, reportReference)
            }
        }.awaitAll()
}

fun parseReport(data: InputStream?, reportReference: ReportReference): ReportReference {
    println("Parsing data of ${reportReference.ticker} ${reportReference?.reportFiles?.xbrlReport}")
    return data?.use {
        try {
            val date = reportReference.dateFiled?.atStartOfDay()!!
            val isInline = reportReference.reportFiles!!.xbrlReport!!.endsWith(".htm")

            val parsedProps = parseProps(
                data,
                date,
                reportReference.formType!!,
                isInline
            )

            val relatedContexts = parsedProps.data.flatMap { it.contexts }.toSet()
            val relatedUnits = parsedProps.data.flatMap { it.valueUnits ?: emptySet() }.toSet()

            val withExtractedData = reportReference.copy(
                contexts = relatedContexts,
                units = relatedUnits,
                problems = if (parsedProps.problems.missingProperties.isEmpty() && parsedProps.problems.suspiciousContexts.isEmpty()) null else parsedProps.problems,
                extractedData = parsedProps.data.flatMap {
                    it.extractedValues
                        .filter { it.unit != null || it.propertyId.startsWith("dei:") }
                        .filter { it.value.length <= 100 }
                }
            )

            withExtractedData.copy(
                processed = true,
                assets = Assets.get(withExtractedData),
                revenue = Revenue.get(withExtractedData),
                eps = Eps.get(withExtractedData),
                liabilities = Liabilities.get(withExtractedData),
                operatingCashFlow = OperatingCashFlow.get(withExtractedData),
                financingCashFlow = FinancingCashFlow.get(withExtractedData),
                investingCashFlow = InvestingCashFlow.get(withExtractedData),
                netIncome = NetIncome.get(withExtractedData),
                fiscalYear = FiscalYearExtractor.get(withExtractedData)?.toLong()
            )
        } catch (e: Exception) {
            println("failed to parse report from ${reportReference.reportFiles!!.xbrlReport!!}.xml (${reportReference.dataUrl})")
            e.printStackTrace()
            reportReference.copy(processed = false)
        }

    } ?: reportReference
}

fun parseProps(data: InputStream, date: LocalDateTime, reportType: String, isInline: Boolean): ReportDataExtractionResult {
    return Report(data, date, reportType, isInline).extractData()
}





