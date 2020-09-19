package fi.avp.edgar

import fi.avp.edgar.util.asyncGetText
import fi.avp.edgar.util.mapAsync
import kotlinx.coroutines.*
import org.junit.Test
import org.litote.kmongo.gt
import java.time.LocalDate
import java.util.concurrent.Executors

class IndexCrawlerKtTest {

    private val taskDispatcher: CoroutineDispatcher = Executors.newFixedThreadPool(16).asCoroutineDispatcher()

    @Test
    fun crawlLatestIndexReturnsCorrectFileRecord() {
        runBlocking(taskDispatcher) {
            Database.filings.deleteMany(Filing::dateFiled gt LocalDate.now().minusDays(3))
            val filings = getFilingsAfter(LocalDate.now().minusDays(3))
                .flatMap {
                    asyncGetText(it.url)
                        .split("\n")
                        .mapNotNull { resolveFilingInfoFromIndexRecord(it) }
                }

            filings.chunked(5).forEach {
                it.mapAsync { it.withBasicFilingData() }
                .awaitAll()
                .forEach {
                    Database.filings.save(it)
                }

                delay(5000)
            }
        }
    }
}

