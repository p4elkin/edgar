package fi.avp.edgar

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import kotlinx.coroutines.runBlocking
import org.bson.types.ObjectId
import org.junit.Test
import org.litote.kmongo.coroutine.coroutine
import org.litote.kmongo.coroutine.toList
import org.litote.kmongo.eq
import org.litote.kmongo.exclude
import org.litote.kmongo.fields
import org.litote.kmongo.reactivestreams.KMongo
import org.litote.kmongo.regex
import strikt.api.expectThat
import java.io.File
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import kotlin.system.measureTimeMillis
import kotlin.test.assertTrue


class ReportTest {

    @Test
    fun v20150331l_contains_eps_properties() {
        val file = File("src/test/resources/v-20150331.xml")
        val extractedData= Report(
            file.inputStream(),
            "10-Q", false
        ).extractData(LocalDate.of(2015, 3, 31).atStartOfDay())

        assertTrue { extractedData.data
            .flatMap { it.extractedValues }
            .filter { it.propertyId.toLowerCase().contains("earningspershare") }.isNotEmpty() }
    }

    @Test
    fun resolveYearToYearChangesForLatestAppleFiling() = runBlocking {
        val latestFiling = Database.getFilingsByTicker("AAPL").maxBy { it.dateFiled!! }
        val updated = scrapeFilingFacts(latestFiling!!)
    }

    @Test
    fun canParse_a_20100430() {
        assertTrue {
            reportDataExtractionResult("a-20100430.xml", 2010, 4, 30).data.isNotEmpty()
        }
    }

    @Test
    fun stz20110531_contains_eps() {
        val file = File("src/test/resources/stz-20110531.xml")
        val extractedData= Report(
            file.inputStream(),
            "10-Q", false
        ).extractData(LocalDate.of(2011, 5, 31).atStartOfDay())

        val epsData = extractedData.data
            .flatMap { it.extractedValues.filter { it.unit != null } }
            .filter { it.propertyId.toLowerCase().contains("earningspershare") }

        assertTrue { epsData.isNotEmpty() }
    }

    @Test
    fun canParse_q1202010_q033120() {
        val extractedData = reportDataExtractionResult(
            "q1202010-q033120.htm", 2020, 3, 31)

        assertTrue { extractedData.data.isNotEmpty() }
    }

    @Test
    fun hasNoProblems_aap_20181006() {
        val extractedData = reportDataExtractionResult("aap-20181006.xml", 2018, 10, 6);

        assertTrue { extractedData.problems.suspiciousContexts.size == 1 && extractedData.problems.missingProperties.size == 3}
    }

    @Test
    fun bdx20130331() {
        val file = File("src/test/resources/bdx-20130331.xml")
        val extractedData= Report(
            file.inputStream(),
            "10-Q", false
        ).extractData(LocalDate.of(2013, 5, 9).atStartOfDay())

        assertTrue{ extractedData.data.flatMap{ it.extractedValues }.find { it.propertyId == "SalesRevenueNet" } != null}
    }

    @Test
    fun d754327d10() {
        val file = File("src/test/resources/d754327d10q.htm")
        val extractedData= Report(
            file.inputStream(),
            "10-Q", true
        ).extractData(LocalDate.of(2019, 6, 30).atStartOfDay())

        assertTrue { extractedData.data.isNotEmpty() }
    }

    @Test
    fun `000091591315000018`() {
        val file = File("src/test/resources/wrk-10k_20190930.htm")
        val extractedData= Report(
            file.inputStream(),
            "10-Q", true
        ).extractData(LocalDate.of(2015, 5, 11).atStartOfDay())
        assertTrue { extractedData.data.isNotEmpty() }
    }

    @Test
    fun abt20111231() {
        val file = File("src/test/resources/abt-20111231.xml")
        val extractedData= Report(
            file.inputStream(),
            "10-K",
            false
        ).extractData(LocalDate.of(2011, 12, 31).atStartOfDay())
        assertTrue { extractedData.problems.suspiciousContexts.isEmpty() }
    }

    @Test
    fun mapCompanyReportRecordsToData() {
        val dataStream = getCompanyReports("AAPL")
        val appleRefs =
            runBlocking {
                Database.getFilingsByTicker("AAPL").map {
                    it to dataStream.get("${it.reference}.xml")
                }.toMap()
            }
        assertTrue { appleRefs.isNotEmpty() }
    }

    @Test
    fun bxp_20130331_doesNotContainUnnecessaryContexts() {
        val reportDataExtractionResult = reportDataExtractionResult(
            "bxp-20130331.xml", 2013, 3, 31)
    }

    @Test
    fun getPreviousYearQuarterFiling() {
        runBlocking {
            Database.filings.find(Filing::_id eq ObjectId("5f29a0eafaaf3c66cf17b177")).first()?.let {
                scrapeFilingFacts(it)?.let {
                    println(it)
                }
            }
        }
    }

    @Test
    fun getAllFilings() {
        runBlocking {
            println(measureTimeMillis {
                Database.getAllFilings()
            })
        }
    }

    @Test
    fun fetchFilingsByTicker() {
        val database = KMongo.createClient().coroutine.getDatabase("sec-report")
        val collection = database.getCollection<Filing>("filings")
        println(measureTimeMillis {
            runBlocking {
                collection
                    .find("{ticker: 'AAPL'}")
                    .projection(fields(exclude(Filing::extractedData)))
                    .sort(BasicDBObject("dateFiled", -1))
                    .skip(0)
                    .limit(40)
                    .toList()
            }
        })
    }

    @Test
    fun calculateAdjustmentsToReconcileNetIncomeToCashflowWallmart2019() {
        runBlocking {
            Database.filings.findOne("{dataUrl: 'https://www.sec.gov/Archives/edgar/data/104169/000010416919000016'}")?.let {
                val calculateReconciliationValues = calculateReconciliationValues(it)
            }
        }
    }

    @Test
    fun parsesOutAllRelevantProperties() {
        runBlocking {
            Database.filings.findOne("{dataUrl: 'https://www.sec.gov/Archives/edgar/data/1775098/000177509820000006'}")?.let {
                it.files?.xbrlData(it.dataUrl!!)?.let {
                    val extractData = Report(it.byteInputStream(StandardCharsets.UTF_8), "10-K", true)
                        .extractData(LocalDate.of(2013, 4, 1).atStartOfDay())
                    assertTrue { extractData.data.find { it.descriptor.id.contains("dei:DocumentFiscalYearFocus") } != null }
                }
            }
        }
    }


    @Test
    fun parseValue() {
//        println(parseValue("101.2", -8.0 ).toBigDecimal().toPlainString())
//        println(parseValue("4,674,071", -3.0 ).toBigDecimal().toPlainString())
//        println(parseValue("2.47", 2.0 ).toBigDecimal().toPlainString())
//        println(parseValue("2.47", 0.0 ).toBigDecimal().toPlainString())
//        println(parseValue("3250000000", -6.0 ).toBigDecimal().toPlainString())
    }

    @Test
    fun getReportDataStream() {
        val dataStream = getCompanyReports("AAPL")
        assertTrue { dataStream.isNotEmpty() }
    }

    @Test
    fun getTickers() {
        assertTrue { runBlocking { Database.getTickers().contains("AAPL") } }
    }


    private fun reportDataExtractionResult(fileName: String, year: Int, month: Int, day: Int): ReportDataExtractionResult {
        val file = File("src/test/resources/${fileName}")
        return Report(
            file.inputStream(),
            "10-Q", fileName.endsWith(".htm")
        ).extractData(LocalDate.of(year, month, day).atStartOfDay())
    }
}
