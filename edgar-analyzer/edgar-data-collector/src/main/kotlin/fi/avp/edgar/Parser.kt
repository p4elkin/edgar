package fi.avp.edgar

import com.mongodb.BasicDBObject
import fi.avp.util.mapAsync
import kotlinx.coroutines.*
import org.litote.kmongo.coroutine.replaceOne

suspend fun scrapeFilingFacts(filing: Filing): Filing = coroutineScope {

    if (filing.dataExtractionStatus == OperationStatus.FAILED) {
        println("Skipping previously failed to be parsed filing ${filing.dataUrl}")
        return@coroutineScope filing
    }

    val actualFiling = async { filing.withBasicFilingData() }
    val annualReportTask = async { filing.getClosestAnnualReport()?.withBasicFilingData() }
    val previousYearFiling = async { filing.getPreviousYearFiling()?.withBasicFilingData() }

    val withYearToYearDiffs = actualFiling.await().withYearToYearDiffs(previousYearFiling.await())
    withYearToYearDiffs.withClosestAnnualReportLink(annualReportTask.await())
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


