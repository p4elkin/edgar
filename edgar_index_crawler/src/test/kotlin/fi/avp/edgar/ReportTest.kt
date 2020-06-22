package fi.avp.edgar

import fi.avp.edgar.data.CompanyRef
import fi.avp.edgar.data.ReportMetadata
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
            file.inputStream(), false,
            ReportMetadata(
                CompanyRef("000119312513300670", "apple_inc"),
                "2012", "QTR2", "10-Q", LocalDate.of(2012, 7, 25).atStartOfDay(), ""
            )
        )

        val resolveProperty = disambiguateProperties(listOf(report.resolveProperty(attrNames.find { it.id == "revenue" }!!)))
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
