package fi.avp.edgar

import fi.avp.edgar.data.attrNames
import org.junit.Test
import java.io.File
import java.time.LocalDate
import kotlin.test.assertTrue


class ReportTest {

    @Test
    fun test() {
        val file = File("src/test/resources/a10-qq1202012282019.htm")
        val report = Report(
            file.inputStream(),
            LocalDate.of(2012, 7, 25).atStartOfDay(),
            "10-Q"
        )

        val props = report.resolveProperty(attrNames.find { it.id == "revenue" }!!)
//        val resolveProperty = disambiguateProperties(
//            getNonAmbiguousContexts(listOf(props)),
//            listOf(props)
//        )
    }

    @Test
    fun `000091591315000018`() {
        val file = File("src/test/resources/000091591315000018.xml")
        val parseProps = parseProps(file.inputStream(), LocalDate.of(2015, 5, 11).atStartOfDay(), "10-Q")
        assertTrue { parseProps.data.isNotEmpty() }
    }

    @Test
    fun abt20111231() {
        val file = File("src/test/resources/abt-20111231.xml")
        val parseProps = parseProps(file.inputStream(), LocalDate.of(2011, 12, 31).atStartOfDay(), "10-K")
        assertTrue { parseProps.problems.suspiciousContexts.isEmpty() }
    }

    @Test
    fun mapCompanyReportRecordsToData() {
        val dataStream = getCompanyReports("AAPL")
        val appleRefs =
            Database.getReportReferences("AAPL").map {
                it to dataStream.get("${it.reference}.xml")
            }.toMap()
        assertTrue { appleRefs.isNotEmpty() }
    }

    @Test
    fun getReportDataStream() {
        val dataStream = getCompanyReports("AAPL")
        assertTrue { dataStream.isNotEmpty() }
    }

    @Test
    fun getTickers() {
        assertTrue { Database.getTickers().contains("AAPL") }
        assertTrue { Database.getSP500Tickers().contains("AAPL") }
    }

    @Test
    fun tickerTest() {
        getCompanyNames().filter {
            getTicker(it) == null
        }.forEach { println(it) }
    }

    @Test
    fun companyNameListContainsTeConnectivityLtd() {
        assertTrue { getCompanyNames().contains("te_connectivity_ltd") }
    }

    @Test
    fun companyNameListContainsCarrierGlobalCorp() {
        assertTrue { getCompanyNames().contains("carrier_global_corp") }
    }
}
