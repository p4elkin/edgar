package fi.avp.edgar

import fi.avp.edgar.util.*
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.litote.kmongo.coroutine.replaceOne
import org.litote.kmongo.dayOfMonth
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.io.InputStream
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.Executors
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPath
import javax.xml.xpath.XPathExpression
import javax.xml.xpath.XPathFactory
import kotlin.collections.forEach
import kotlin.collections.map
import kotlin.let

@EnableScheduling
@SpringBootApplication
open class RssCrawlerApplication {


    @Component
    open class CurrentIndexCrawler {

        var daysBack: Long = -1

        @Scheduled(fixedRate = 24 * 60 * 60 * 1000)
        fun crawl() {
            // TODO - it's a hack for restarts
            if (daysBack < 0) {
                daysBack = 21
            } else {
                daysBack = 3
            }
            runBlocking(Executors.newFixedThreadPool(4).asCoroutineDispatcher()) {
                val newFilings = getLatestFilings(LocalDate.now().minusDays(daysBack))

                println("About to parse ${newFilings.size} new filings")
                newFilings
                    .map { Database.tryResolveExisting(it) }
                    .forEach {

                        println("Will now parse ${it.ticker} ${it.formType}")
                        // resolve everything up to the year to year changes
                        scrapeFilingFacts(it).let {
                            if (it._id != null) {
                                Database.filings.replaceOne(it)
                            } else {
                                Database.filings.save(it)
                            }

                            try {
                                parseIncomeStatement(it)?.let { Database.income.save(it) }
                                parseOperationsStatement(it)?.let { Database.operations.save(it) }
                                parseBalanceSheet(it)?.let { Database.balance.save(it) }
                                parseCashFlow(it)?.let { Database.cashflow.save(it) }
                            } catch (e: Exception) {
                                println("failed to parse statements for ${it.dataUrl}")
                                e.printStackTrace()
                            }
                        }

                        delay(5000)
                    }
            }
        }
    }
}


fun main(args: Array<String>) {
    runApplication<RssCrawlerApplication>(*args)
}

suspend fun getLatestFilings(since: LocalDate): List<Filing> {
    val currentDate = LocalDate.now()
    val currentMonth = since.month
    val currentYear = since.year

    val firstDayOfMonthSince = LocalDate.of(currentYear, currentMonth, 1)

    return generateSequence(firstDayOfMonthSince) { date -> date.plusMonths(1) }
        .takeWhile { currentDate >= it }
        .toList()
        .mapAsync {
            val monthValue = it.monthValue.let { monthNumber ->
                if (monthNumber < 10) {
                    "0$monthNumber"
                } else {
                    monthNumber.toString()
                }
            }

            asyncGet("https://www.sec.gov/Archives/edgar/monthly/xbrlrss-${it.year}-${monthValue}.xml").let {
                if (!it.isSuccessful) {
//                    println("Failed to fetch RSS data: ${response.message} ${response.code}")
                    emptyList()
                } else {
                    parseRss(it.body!!.byteStream()).filter { it.dateFiled!!.atStartOfDay() >= since.atStartOfDay() }
                }
            }
        }.awaitAll().flatten()
        .filter { it.formType in setOf("10-K", "10-Q") }
        .filter { it.dateFiled!! >= since }
}

val xpath: XPath by lazy {
    XPathFactory.newInstance().newXPath()
}

private val rssEntries: XPathExpression = xpath.compile("//item")
suspend fun parseRss(rssContent: InputStream): List<Filing> {
    return try {

        val builderFactory = DocumentBuilderFactory.newInstance()
        builderFactory.isNamespaceAware = true
        val document = builderFactory.newDocumentBuilder().parse(rssContent)

        rssEntries.nodeSet(document).map { filing ->
            val xbrlRecord = filing.find("xbrlFiling")!!
            val link = filing.find("link")?.textContent ?: ""
            val dataUrl = link.substringBeforeLast("/").replace("https", "http")
            val cik = xbrlRecord.find("cikNumber")!!.textContent.toInt()
            val filingDate = xbrlRecord.find("filingDate")!!.textContent.let {
                LocalDate.parse(it, DateTimeFormatter.ofPattern("MM/dd/yyyy"))
            }
            Filing(
                ticker = Database.getCompanyList().find { it.cik.contains(cik) }?.primaryTicker ?: "",
                companyName = xbrlRecord.find("companyName")!!.textContent,
                formType = xbrlRecord.find("formType")!!.textContent.toUpperCase(),
                fileName = link,
                dataUrl = dataUrl,
                cik = cik.toLong(),
                dateFiled = filingDate
            )
        }
    } catch (e: Exception) {
        e.printStackTrace()
        emptyList()
    }
}
