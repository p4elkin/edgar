package fi.avp.edgar.mining

import fi.avp.edgar.CompanyInfo
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.w3c.dom.Document
import java.io.BufferedReader
import java.io.InputStream
import java.nio.charset.StandardCharsets
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPath
import javax.xml.xpath.XPathFactory
import kotlin.system.measureTimeMillis

val propertyPattern = Regex(".*_(.+),")
fun main() {
    Database.getSP500Companies().forEach {
        val companyInfo = it
        val doneIn = measureTimeMillis {
            val filings =
                it.cik.flatMap { Database.getFilingsByCik(it.toString()) }.filter { it.formType == "10-K" }

            val reportData = getCompanyReports(it.primaryTicker)

            filings.forEach {
                extractCashflowReconciliationData(it, companyInfo, reportData)
            }
        }
    }
}

private fun extractCashflowReconciliationData(filing: Filing, companyInfo: CompanyInfo, reportData: Map<String, InputStream>) {
    val cashFlowStatment = filing.files?.cashFlow
    if (cashFlowStatment == null) {
        println("${companyInfo.primaryTicker} ${filing.dateFiled} ${filing.dataUrl}")
    }

    cashFlowStatment?.let {
        try {
            val cashflowFileName = "cashflow-${filing.files!!.xbrlReport!!}"
            val content = reportData[cashflowFileName]
            content?.let {
                val xml = BufferedReader(it.reader(StandardCharsets.UTF_8))
                    .readText()
                    .replace("<link rel=\"stylesheet\" type=\"text/css\" href=\"report.css\">", "")
                    .replace("'", "")
                    .replace("<br>", "")

                val parsedHtml = Jsoup.parse(xml)

                val attrId = "gaap_AdjustmentsToReconcileNetIncomeLossToCashProvidedByUsedInOperatingActivitiesAbstract"
                val reconciliationNode: Element? = parsedHtml.selectFirst("tr > td > a[onclick*='$attrId']")
                reconciliationNode?.parents()?.find { it.`is`("tr") }?.let {
                    val allRows = generateSequence(it.nextElementSibling()) { it.nextElementSibling() }
                        .map {
                            val propertyLink = it.selectFirst("td > a")
                            propertyPattern.find(propertyLink.attr("onclick"))
                                ?.groups
                                ?.let {
                                    it[1]?.value
                                } ?: ""
                        }
                        .filter { it.isNotEmpty() }
                        .takeWhile { !it.contains("Abstract") }
                        .toList()
                    val reconciliationValues = allRows.map { propertyId ->
                        propertyId to filing.extractedData?.find { it.propertyId.substringAfterLast(":") == propertyId }?.value
                    }
                    reconciliationValues
                }
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
}

class Cashflow(val content: InputStream) {
    fun extract(): Any? {
        val expr =
            "//*[contains(@onclick, 'gaap_AdjustmentsToReconcileNetIncomeLossToCashProvidedByUsedInOperatingActivitiesAbstract')]"
        return xpath.compile(expr).evaluate(reportDoc)
    }

    private val xpath: XPath by lazy {
        XPathFactory.newInstance().newXPath()
    }

    private val reportDoc: Document by lazy {
        val builderFactory = DocumentBuilderFactory.newInstance()
        builderFactory.isNamespaceAware = true

        val builder = builderFactory.newDocumentBuilder()
        try {
            builder.parse(content)
        } catch (e: Exception) {
            e.printStackTrace()
            throw RuntimeException(e)
        }
    }

}
