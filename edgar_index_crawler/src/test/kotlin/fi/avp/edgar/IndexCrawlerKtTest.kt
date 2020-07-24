package fi.avp.edgar

import fi.avp.util.asyncGetText
import fi.avp.util.mapAsync
import kotlinx.coroutines.*
import org.junit.Test
import org.litote.kmongo.Data
import org.litote.kmongo.gt
import org.litote.kmongo.save
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.Executors

class IndexCrawlerKtTest {

    private val taskDispatcher: CoroutineDispatcher = Executors.newFixedThreadPool(16).asCoroutineDispatcher()

    @Test
    fun crawlLatestIndexReturnsCorrectFileRecord() {
        runBlocking(taskDispatcher) {
            Database.reportIndex.deleteMany(ReportReference::dateFiled gt LocalDate.now().minusDays(3))
            val filings = getFilingsAfter(LocalDate.now().minusDays(3))
                .flatMap {
                    asyncGetText(it.url)
                        .split("\n")
                        .mapNotNull { resolveFilingInfoFromIndexRecord(it) }
                }

            filings.chunked(5).forEach {
                it.mapAsync {
                    val filingRef = it.copy(reportFiles = fetchRelevantFileNames(it))
                    downloadSingleReport(filingRef)?.xbrl?.let {
                        parseReport(it.byteInputStream(StandardCharsets.UTF_8), filingRef)
                    } ?: filingRef
                }
                .awaitAll()
                .forEach {
                    Database.reportIndex.save(it)
                }

                delay(5000)
            }
        }
    }
}

