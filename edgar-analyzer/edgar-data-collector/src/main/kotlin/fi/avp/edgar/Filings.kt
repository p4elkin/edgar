package fi.avp.edgar

import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.retry
import com.mongodb.BasicDBObject
import fi.avp.edgar.util.asyncGet
import fi.avp.edgar.util.asyncJson
import fi.avp.edgar.util.long
import fi.avp.edgar.util.text
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import org.litote.kmongo.and
import org.litote.kmongo.eq
import org.litote.kmongo.lt
import org.litote.kmongo.or
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
import java.util.zip.ZipInputStream
import kotlin.time.measureTimedValue

private val reportCandidateBlackList = Regex(".*_(cal|pre|def|lab)\\..*")

suspend fun Filing.getClosestAnnualReport(): Filing? {
    val date = dateFiled!!
    val sortCriteria = BasicDBObject("dateFiled", -1)
    if (formType == "10-K") {
        return this
    }
    return Database.filings
        .find(
            and(Filing::dateFiled lt date, Filing::formType eq "10-K"),
            or(Filing::ticker eq ticker, Filing::cik eq cik)
        )
        .sort(sortCriteria)
        .first()
}

suspend fun Filing.getPreviousYearFiling(): Filing? {
    val date = dateFiled!!
    val sortCriteria = BasicDBObject("dateFiled", -1)
    return Database.filings
        .find(and(Filing::dateFiled lt date, or(Filing::cik eq cik, Filing::ticker eq ticker)))
        .sort(sortCriteria)
        .skip(3)
        .first()
}

val reportPattern = Regex("^(.+)-.+")
suspend fun Filing.withTicker(): Filing {
    val resolveTickerFromReportFileName: suspend () -> String? = {
        this.withFiles().files?.xbrlReport?.let {
            reportPattern.find(it)?.let { reportFileNameMatchResult ->
                reportFileNameMatchResult.groups[1]!!.value
            }
        }
    }

    val ticker = Database.getCompanyList().find { it.cik.contains(cik!!.toInt()) }
        ?.primaryTicker
        ?: resolveTickerFromReportFileName()

    return copy(ticker = ticker)
}

suspend fun Filing.withFiles(): Filing {
    return when (fileResolutionStatus) {
        OperationStatus.PENDING, OperationStatus.FAILED -> {
            try {
                doResolveFiles();
            } catch (e: java.lang.Exception) {
                e.printStackTrace()
                copy(fileResolutionStatus = OperationStatus.FAILED)
            }
        } else -> this
    }
}

suspend fun Filing.withExtractedMetrics(): Filing {
    return withBasicFilingData().copy(
        cashIncome = calculateReconciliationValues(this),
        equity = Equity.get(this),
        assets = Assets.get(this),
        revenue = Revenue.get(this),
        eps = Eps.get(this),
        liabilities = Liabilities.get(this),
        operatingCashFlow = OperatingCashFlow.get(this),
        financingCashFlow = FinancingCashFlow.get(this),
        investingCashFlow = InvestingCashFlow.get(this),
        netIncome = NetIncome.get(this),
        fiscalYear = FiscalYearExtractor.get(this)?.toLong(),
        sharesOutstanding = SharesOutstandingExtractor.get(this)?.toLong()
    )
}

suspend fun Filing.withBasicFilingData(): Filing {
    return when (dataExtractionStatus) {
        OperationStatus.DONE, OperationStatus.MISSING -> this
        else -> withFiles().doParseXBRLProperties()
    }
}

suspend fun Filing.withYearToYearDiffs(): Filing = coroutineScope {
    withYearToYearDiffs(getPreviousYearFiling())
}

suspend fun Filing.withClosestAnnualReportLink(): Filing = coroutineScope {
    withClosestAnnualReportLink(getClosestAnnualReport())
}

suspend fun Filing.withClosestAnnualReportLink(closesAnnualFiling: Filing?): Filing {
    return withBasicFilingData().copy(
            closestYearReportId = closesAnnualFiling?._id,
            latestRevenue = closesAnnualFiling?.revenue?.value)
}

suspend fun Filing.withYearToYearDiffs(previous: Filing?): Filing = coroutineScope {
    previous?.let { previousYearFiling ->
        val withData = withBasicFilingData()
        val dataWithDeltas = withData.extractedData?.let {
            it.map { value ->
                val previousYearValue = previousYearFiling.extractedData?.findLast { it.propertyId == value.propertyId }
                val delta = previousYearValue?.let {
                    value.numericValue() - it.numericValue()
                }
                value.copy(delta = delta)
            }
        }

        val withMetrics = withData.let {
            it.copy(
                eps = it.eps?.calculateYearToYear(previousYearFiling.eps),
                extractedData = dataWithDeltas,
                assets = it.assets?.calculateYearToYear(previousYearFiling.assets),
                revenue = it.revenue?.calculateYearToYear(previousYearFiling.revenue),
                netIncome = it.netIncome?.calculateYearToYear(previousYearFiling.netIncome),
                investingCashFlow = it.investingCashFlow?.calculateYearToYear(previousYearFiling.investingCashFlow),
                operatingCashFlow = it.operatingCashFlow?.calculateYearToYear(previousYearFiling.operatingCashFlow),
                financingCashFlow = it.financingCashFlow?.calculateYearToYear(previousYearFiling.financingCashFlow),
                liabilities = it.liabilities?.calculateYearToYear(previousYearFiling.liabilities))
        }

        withMetrics
    } ?: this@withYearToYearDiffs
}

suspend fun Filing.doResolveFiles() = retry(limitAttempts(5)) {
    suspend fun resolveXBRLFilename(reportBaseUrl: String?, xbrlZipName: String): String? {
        println("Resolving related file names of ${ticker} on ${dateFiled}")
        val xbrlZip = asyncGet("$reportBaseUrl/${xbrlZipName}")
        return if (xbrlZip.isSuccessful) {
            xbrlZip.body!!.byteStream().use {
                ZipInputStream(it.buffered()).use { zipInputStream ->
                    generateSequence { zipInputStream.nextEntry }
                        .filter { !reportCandidateBlackList.matches(it.name) }
                        .filter {
                            val reportFileName = it.name
                            val companyName = companyName!!.toLowerCase()
                            reportFileName.endsWith("xml") ||
                                    (reportFileName.endsWith("htm") &&
                                            // Filter out potentially large extra files
                                            (companyName.contains("ex") ||
                                                    !reportFileName.contains("ex")))
                        }
                        .map { it.name to it.size }
                        .maxBy { it.second }
                        ?.first
                }
            }
        } else null
    }

    val reportBaseUrl = dataUrl
    val index = asyncJson("$reportBaseUrl/index.json")

    val humanReadableReportFileName = index["directory"]["item"]
        .filter { it.text("name").endsWith(".htm") }
        .filter { (!it.text("name").contains("ex")) }
        .maxBy { it.long("size") }
        ?.text("name")

    val xbrlZipUrl = if (fileName!!.endsWith(".txt")) {
        dataUrl + "/" + fileName!!.substringAfterLast("/").replace(".txt", "-xbrl.zip")
    } else {
        fileName!!.replace("-index.htm", "-xbrl.zip")
    }

    val xbrlZipName = xbrlZipUrl.substringAfterLast("/")
    val xbrlReport = resolveXBRLFilename(reportBaseUrl, xbrlZipName)

    if (xbrlReport == null) {
        copy(fileResolutionStatus = OperationStatus.MISSING)
    } else {
        val filingSummary = FilingSummary(asyncGet("$reportBaseUrl/FilingSummary.xml").body!!.byteStream())
        copy(fileResolutionStatus = OperationStatus.DONE,
            files = Filing.ReportFiles(
                humanReadableReportFileName,
                xbrlReport,
                filingSummary.getConsolidatedStatementOfCashFlow(),
                filingSummary.getConsolidatedStatementOfIncome(),
                filingSummary.getConsolidatedStatementOfOperation(),
                filingSummary.getConsolidatedBalanceSheet(),
                filingSummary.getFinancialSummary()
            )
        )
    }
}

private val taskDispatcher: CoroutineDispatcher = Executors.newFixedThreadPool(8).asCoroutineDispatcher()
suspend fun Filing.doParseXBRLProperties(): Filing = coroutineScope {
    when (fileResolutionStatus) {
        OperationStatus.MISSING -> {
            copy(dataExtractionStatus = OperationStatus.MISSING)
        }
        OperationStatus.FAILED -> {
            copy(dataExtractionStatus = OperationStatus.PENDING)
        }
        else -> {
            files?.xbrlData(dataUrl!!)?.let {
                try {
                    println("Parsing data of ${ticker} ${files!!.xbrlReport} on ${Thread.currentThread().name}")
                    val date = dateFiled?.atStartOfDay()!!
                    val isInline = files!!.xbrlReport!!.endsWith(".htm")


                    val parsedProps = withContext(taskDispatcher) {
                        val (value, time) = measureTimedValue {
                            Report(it.byteInputStream(StandardCharsets.UTF_8), formType!!, isInline).extractData(date)
                        }

                        println("Parsed data of ${ticker} ${files!!.xbrlReport} in ${time}ms on ${Thread.currentThread().name}")
                        value
                    }

                    val relatedContexts = parsedProps.data.flatMap { it.contexts }.toSet()
                    val relatedUnits = parsedProps.data.flatMap { it.valueUnits ?: emptySet() }.toSet()

                    copy(
                        dataExtractionStatus = OperationStatus.DONE,
                        contexts = relatedContexts,
                        units = relatedUnits,
                        problems = if (parsedProps.problems.missingProperties.isEmpty() && parsedProps.problems.suspiciousContexts.isEmpty()) null else parsedProps.problems,
                        extractedData = parsedProps.data.flatMap {
                            it.extractedValues
                                .filter { it.unit != null || it.propertyId.startsWith("dei:") }
                                .filter { it.value.length <= 100 }
                        })
                } catch (e: Exception) {
                    println("failed to parse report from ${files!!.xbrlReport!!}.xml (${dataUrl})")
                    e.printStackTrace()
                    copy(dataExtractionStatus = OperationStatus.FAILED)
                }
            } ?: copy(dataExtractionStatus = OperationStatus.MISSING)
        }
    }
}
