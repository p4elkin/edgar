package fi.avp.edgar

import com.mongodb.BasicDBObject
import fi.avp.edgar.util.forEachAsync
import fi.avp.edgar.util.mapAsync
import kotlinx.coroutines.*
import org.litote.kmongo.coroutine.replaceOne

suspend fun scrapeFilingFacts(filing: Filing): Filing = coroutineScope {
    try {
        val filingWithResolvedFiles = filing.withFiles()

        val actualFiling = async { filingWithResolvedFiles.withBasicFilingData().withExtractedMetrics() }
        val annualReportTask = async { filingWithResolvedFiles.getClosestAnnualReport()?.withBasicFilingData() }
        val previousYearFiling = async { filingWithResolvedFiles.getPreviousYearFiling()?.withBasicFilingData() }

        val withYearToYearDiffs = actualFiling.await().withYearToYearDiffs(previousYearFiling.await())
        withYearToYearDiffs.withClosestAnnualReportLink(annualReportTask.await())
    } catch (e: Exception) {
        println("Failed to parse filing ${filing.dataUrl} due to [${e.message}]")
        filing
    }
}

fun main(args: Array<String>) = runBlocking {
    updateFilingsConcurrently("{formType: '10-K'}") {
        coroutineScope {
            try {
                parseIncomeStatement(it)?.let { Database.income.save(it) }
                parseOperationsStatement(it)?.let { Database.operations.save(it) }
                parseBalanceSheet(it)?.let { Database.balance.save(it) }
                parseCashFlow(it)?.let { Database.cashflow.save(it) }
            } catch (e: Exception) {
                println("failed to parse statements for ${it.dataUrl}")
                e.printStackTrace()
            }

            println("Scraping for ${it.formType} of ${it.companyName} on ${it.dateFiled}")
            val withAllFacts = scrapeFilingFacts(
                // make sure to re-fetch all the metrics
                it.copy(dataExtractionStatus = OperationStatus.PENDING)
            )


            Database.filings.replaceOne(withAllFacts)
        }
    }
}


