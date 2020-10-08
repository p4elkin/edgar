package fi.avp.edgar

import com.mongodb.BasicDBObject
import fi.avp.edgar.util.mapAsync
import kotlinx.coroutines.*
import org.litote.kmongo.coroutine.replaceOne

suspend fun scrapeFilingFacts(filing: Filing): Filing = coroutineScope {
    try {
        if (filing.dataExtractionStatus == OperationStatus.FAILED) {
            println("Skipping previously failed to be parsed filing ${filing.dataUrl}")
            return@coroutineScope filing
        }

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
    Database.filings.find(
        "{formType: '10-Q', fileResolutionStatus: 'DONE', dataExtractionStatus: {\$ne: 'DONE'}}")
        .sort(BasicDBObject("dateFiled", -1)).toList()
        .chunked(20)
        .forEach {
            it.mapAsync { it.withBasicFilingData() }.awaitAll().forEach {
                Database.filings.replaceOne(it)
            }
        }
}


