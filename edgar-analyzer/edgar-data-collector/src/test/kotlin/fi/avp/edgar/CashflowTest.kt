package fi.avp.edgar

import kotlinx.coroutines.runBlocking
import org.junit.Test

open class CashflowTest {

    @Test
    fun aflacReportTest() {
        runBlocking {
            val url = "{dataUrl: 'https://www.sec.gov/Archives/edgar/data/4977/000000497716000259'}"
            Database.filings.findOne(url)?.let {
                parseCashFlow(it)
            }
        }
    }
    open fun removeSpecialCharacters(value: String?): String? {
        return value?.replace("[\"[^\\p{IsAlphabetic}\\p{IsDigit}\\p{IsPunctuation}]]".toRegex(), " ")
                ?.replace("\\\\".toRegex(), "")
                ?.trim()
                ?.replace("\\s+".toRegex(), " ")
    }

    @Test
    fun adskReportTest() {
        runBlocking {
            val url = "{dataUrl: 'https://www.sec.gov/Archives/edgar/data/769397/000076939717000014'}"
            Database.filings.findOne(url)?.let {
                parseCashFlow(it)
            }
        }
    }

    @Test
    fun xmlCashflowStatementWithMessedUpSectionResolution() {
        runBlocking {
            val url = "{dataUrl: 'https://www.sec.gov/Archives/edgar/data/1051470/000119312510031419'}"
            Database.filings.findOne(url)?.let {
                parseCashFlow(it)
            }
        }
    }

    @Test
    fun cashflowContainsReconciliationDataWhichIsNotParsed() {
        runBlocking {
            val url = "{dataUrl: 'https://www.sec.gov/Archives/edgar/data/1037949/000119312510032428'}"
            Database.filings.findOne(url)?.let {
                parseCashFlow(it)
            }
        }
    }

    @Test
    fun valuesMessedUp() {
        runBlocking {
            val url = "{dataUrl: 'https://www.sec.gov/Archives/edgar/data/1002037/000143774914022308'}"
            Database.filings.findOne(url)?.let {
                parseCashFlow(it)
            }
        }
    }

    @Test
    fun resolvesMoreCellValuesThanChildElementsInRow_wrong_cashflow() {
        runBlocking {
            val url = "{dataUrl: 'https://www.sec.gov/Archives/edgar/data/1216596/000121659615000007'}"
            Database.filings.findOne(url)?.let {
                parseCashFlow(it)
            }
        }
    }

    @Test
    fun balanceSheetHasColumnHeadersWithWeirdColSpanInfo() {
        runBlocking {
            val url = "{dataUrl: 'https://www.sec.gov/Archives/edgar/data/1131554/000113155418000025'}"
            Database.filings.findOne(url)?.let {
                parseBalanceSheet(it)
            }
        }
    }

    @Test
    fun weird() {
        runBlocking {
            val url = "{dataUrl: 'https://www.sec.gov/Archives/edgar/data/1041514/000106299318003721'}"
            Database.filings.findOne(url)?.let {
                parseBalanceSheet(it)
            }
        }
    }

    @Test
    fun columnValueDoesNotSkipSupCells() {
        runBlocking {
            val url = "{dataUrl: 'https://www.sec.gov/Archives/edgar/data/1646587/000164658716000065'}"
            Database.filings.findOne(url)?.let {
                parseCashFlow(it)
            }
        }
    }

    @Test
    fun noCashflowStatementFound() {
        runBlocking {
            val url = "{dataUrl: 'https://www.sec.gov/Archives/edgar/data/1496454/000119312515109598'}"
            Database.filings.findOne(url)?.let {
                val report = parseCashFlow(it)
                report?.sections
            }
        }
    }

    @Test
    fun emptyValuesInReconciliationCells() {
        runBlocking {
            val url = "{dataUrl: 'https://www.sec.gov/Archives/edgar/data/1534504/000153450414000011'}"
            Database.filings.findOne(url)?.let {
                val report = parseCashFlow(it)
                report?.sections
            }
        }
    }
}

