package fi.avp.edgar.mining

import com.mongodb.BasicDBObject
import fi.avp.util.mapAsync
import kotlinx.coroutines.*
import org.litote.kmongo.and
import org.litote.kmongo.eq
import org.litote.kmongo.lt
import org.litote.kmongo.or
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.util.concurrent.Executors
import kotlin.system.measureTimeMillis

private val taskDispatcher: CoroutineDispatcher = Executors.newFixedThreadPool(16).asCoroutineDispatcher()

@SpringBootApplication(scanBasePackages = ["fi.avp.edgar.mining"])
@Configuration
open class LargeCapParser {

    @Bean
    open fun commandLineRunner(ctx: ApplicationContext): CommandLineRunner? {
        return CommandLineRunner { args ->
            runBlocking {
                val latestFilings = Database.getLatestFilings(6)
                latestFilings.map {
                    scrapeFilingFacts(it)
                }.forEach {
                    Database.filings.updateOne(Filing::dataUrl eq it.dataUrl, it)
                }
            }
        }
    }
}

suspend fun scrapeFilingFacts(filing: Filing): Filing = coroutineScope {

    if (filing.dataExtractionStatus == OperationStatus.FAILED) {
        println("Skipping previously failed to be parsed filing ${filing.dataUrl}")
        return@coroutineScope filing
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

    suspend fun getClosestAnnualReport(filing: Filing): Filing? {
        val date = filing.dateFiled!!
        val sortCriteria = BasicDBObject("dateFiled", -1)
        if (filing.formType == "10-K") {
            return filing
        }
        return Database.filings
            .find(and(
                Filing::dateFiled lt date, Filing::formType eq "10-K"),
                or(Filing::ticker eq filing.ticker, Filing::cik eq filing.cik))
            .sort(sortCriteria)
            .first()?.let {
                collectFilingData(it)
            }
    }

    val actualFiling = async { collectFilingData(filing) }
    val annualReportTask = async { getClosestAnnualReport(filing) }
    val previousYearFiling = async { getPreviousYearFiling(filing) }

    val withYearToYear = previousYearFiling.await()?. let {
        resolveYearToYearChanges(actualFiling.await(), it)
    } ?: actualFiling.await()

    val annualReport = annualReportTask.await()
    withYearToYear.copy(
        closestYearReportId = annualReport?._id,
        latestRevenue = annualReport?.revenue?.value,
        yearToYearUpdate = OperationStatus.DONE)
}

fun main(args: Array<String>) {
    runApplication<LargeCapParser>(*args)
}

suspend fun resolveYearToYearChanges(actualFiling: Filing, previousYearFiling: Filing): Filing = coroutineScope {

    val dataWithDeltas = actualFiling.extractedData?.let {
        it.map { value ->
            val previousYearValue = previousYearFiling?.extractedData?.findLast { it.propertyId == value.propertyId }
            val delta = previousYearValue?.let {
                value.numericValue() - it.numericValue()
            }
            value.copy(delta = delta)
        }
    }

    actualFiling.let {
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


/**
 * Collect and save if needed
 */
suspend fun collectFilingData(filing: Filing): Filing {
    val filingWithFiles = filing.copy(files = fetchRelevantFileNames(filing))

    return if (filing.extractedData?.isEmpty() != false) {
        delay(5000)
        fetchXBRLData(filingWithFiles).xbrl?.let {
            val filing = parseFiling(it.byteInputStream(StandardCharsets.UTF_8), filingWithFiles)
            Database.filings.save(filing)
            filing
        } ?: filingWithFiles
    } else {
        filingWithFiles
    }
}

private suspend fun parseLargeCapReports() {
    val sP500Companies = Database.getSP500Companies()
    sP500Companies.forEach {
        val doneIn = measureTimeMillis {
            val filings = it.cik
                .flatMap { Database.getFilingsByCik(it.toString()) }
                .filter { it.formType == "10-K" }

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
            val data = filing.files?.xbrlReport?.let { reportData[it] }
            if (data == null) {
                println("missing report data for: ${filing.reference}")
                filing
            } else {
                parseFiling(data, filing)
            }
        }.awaitAll()
}

fun parseFiling(data: InputStream?, filing: Filing): Filing {
    println("Parsing data of ${filing.ticker} ${filing?.files?.xbrlReport}")
    return data?.use {
        try {
            val date = filing.dateFiled?.atStartOfDay()!!
            val isInline = filing.files!!.xbrlReport!!.endsWith(".htm")

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
                dataExtractionStatus = OperationStatus.DONE,
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
            println("failed to parse report from ${filing.files!!.xbrlReport!!}.xml (${filing.dataUrl})")
            e.printStackTrace()
            filing.copy(processed = false, dataExtractionStatus = OperationStatus.FAILED)
        }
    } ?: filing
}

fun parseProps(data: InputStream, date: LocalDateTime, reportType: String, isInline: Boolean): ReportDataExtractionResult {
    return Report(data, reportType, isInline).extractData(date)
}





