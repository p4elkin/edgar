package fi.avp.edgar.mining

import com.mongodb.BasicDBObject
import fi.avp.util.mapAsync
import kotlinx.coroutines.*
import org.bson.codecs.pojo.annotations.BsonId
import org.litote.kmongo.*
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.io.InputStream
import java.nio.charset.StandardCharsets
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
//                parseLargeCapReports()

                val latestFilings = Database.getLatestFilings(6)
                latestFilings.map {
                    resolveYearToYearChanges(it)
                }.forEach {
                    Database.filings.updateOne(it)
                }
            }
        }
    }

}

fun main(args: Array<String>) {
    runApplication<LargeCapParser>(*args)
}

suspend fun resolveYearToYearChanges(filing: Filing): Filing = coroutineScope {
    val actualFiling = async { collectFilingData(filing) }
    val previousYearFiling = getPreviousYearFiling(filing)

    val dataWithDeltas = actualFiling.await().extractedData?.let {
        it.map { value ->
            val previousYearValue = previousYearFiling?.extractedData?.findLast { it.propertyId == value.propertyId }
            val delta = previousYearValue?.let {
                value.numericValue() - it.numericValue()
            }
            value.copy(delta = delta)
        }
    }

    actualFiling.await().let {
        it.copy(
            eps = it.eps?.calculateYearToYear(previousYearFiling?.eps),
            extractedData = dataWithDeltas,
            assets = it.assets?.calculateYearToYear(previousYearFiling?.assets),
            revenue = it.revenue?.calculateYearToYear(previousYearFiling?.revenue),
            netIncome = it.netIncome?.calculateYearToYear(previousYearFiling?.netIncome),
            investingCashFlow = it.investingCashFlow?.calculateYearToYear(previousYearFiling?.investingCashFlow),
            operatingCashFlow = it.operatingCashFlow?.calculateYearToYear(previousYearFiling?.operatingCashFlow),
            financingCashFlow = it.financingCashFlow?.calculateYearToYear(previousYearFiling?.financingCashFlow),
            liabilities = it.liabilities?.calculateYearToYear(previousYearFiling?.liabilities)
        )
    }
}

suspend fun getPreviousYearFiling(filing: Filing): Filing? {
    val date = filing.dateFiled!!
    val sortCriteria = BasicDBObject("dateFiled", -1)
    return Database.filings
        .find(and(Filing::dateFiled lt date.minusDays(300), Filing::cik eq filing.cik))
        .sort(sortCriteria)
        .first()?.let {
            collectFilingData(it)
        }
}

/**
 * Collect and save if needed
 */
suspend fun collectFilingData(filing: Filing): Filing {
    return if (filing.processed) {
        filing
    } else {
        val filingWithFiles = filing.copy(reportFiles = fetchRelevantFileNames(filing))
        delay(5000)

        downloadSingleReport(filingWithFiles)?.xbrl?.let {
            val filing = parseFiling(it.byteInputStream(StandardCharsets.UTF_8), filingWithFiles)
            Database.filings.save(filing)
            filing
        } ?: filingWithFiles
    }
}

private suspend fun parseLargeCapReports() {
    val sP500Companies = Database.getSP500Companies()
    sP500Companies.forEach {
        val doneIn = measureTimeMillis {
            val filings = it.cik.flatMap {
                Database.getFilingsByCik(
                    it.toString()
                )
            }.filter { it.formType == "10-K" }

            println("Parsing data of ${it.tickers} (${filings.size} files)")

            val reportData = getCompanyReports(it.primaryTicker)
            val parsedReports = withContext(taskDispatcher) {
                parseReports(filings, reportData)

            }

            println("saving ${filings.size} reports")
            parsedReports.forEach {
                Database.filings.save(it)
            }
        }
        println("parsed $it in $doneIn ms")
    }
}

suspend fun parseReports(filings: List<Filing>, reportData: Map<String, InputStream>): List<Filing> {
    return filings
        .mapAsync { filing ->
            println("resolving ${filing.dataUrl}")
            val data = filing.reportFiles?.xbrlReport?.let { reportData[it] }
            if (data == null) {
                println("missing report data for: ${filing.reference}")
                filing
            } else {
                parseFiling(data, filing)
            }
        }.awaitAll()
}

fun parseFiling(data: InputStream?, filing: Filing): Filing {
    println("Parsing data of ${filing.ticker} ${filing?.reportFiles?.xbrlReport}")
    return data?.use {
        try {
            val date = filing.dateFiled?.atStartOfDay()!!
            val isInline = filing.reportFiles!!.xbrlReport!!.endsWith(".htm")

            val parsedProps = parseProps(
                data,
                date,
                filing.formType!!,
                isInline)

            val relatedContexts = parsedProps.data.flatMap { it.contexts }.toSet()
            val relatedUnits = parsedProps.data.flatMap { it.valueUnits ?: emptySet() }.toSet()

            val withExtractedData = filing.copy(
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
            println("failed to parse report from ${filing.reportFiles!!.xbrlReport!!}.xml (${filing.dataUrl})")
            e.printStackTrace()
            filing.copy(processed = false)
        }

    } ?: filing
}

fun parseProps(data: InputStream, date: LocalDateTime, reportType: String, isInline: Boolean): ReportDataExtractionResult {
    return Report(data, reportType, isInline).extractData(date)
}





