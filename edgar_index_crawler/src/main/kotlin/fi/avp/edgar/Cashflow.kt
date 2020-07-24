package fi.avp.edgar

import it.skrape.core.htmlDocument
import it.skrape.selects.html5.td
import it.skrape.selects.html5.tr
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.jsoup.safety.Whitelist
import org.w3c.dom.Document
import org.xml.sax.ErrorHandler
import org.xml.sax.SAXParseException
import java.io.BufferedReader
import java.io.InputStream
import java.io.Reader
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPath
import javax.xml.xpath.XPathFactory
import kotlin.system.measureTimeMillis

val propertyPattern = Regex(".*gaap_(.+),")
fun main() {
    Database.getSP500Companies().forEach {
        val companyInfo = it
        val doneIn = measureTimeMillis {
            val reportReferences =
                it.cik.flatMap { Database.getReportReferencesByCik(it.toString()) }.filter { it.formType == "10-K" }

            val reportData = getCompanyReports(it.primaryTicker)

            reportReferences.forEach {
                val filing = it
                val cashFlowStatment = it.reportFiles?.cashFlow
                if (cashFlowStatment == null) {
                    println("${companyInfo.primaryTicker} ${it.dateFiled} ${it.dataUrl}")
                }

                cashFlowStatment?.let {
                    try {
                        val cashflowFileName = "cashflow-${filing.reportFiles!!.xbrlReport!!}"
                        val content = reportData[cashflowFileName]
                        content?.let {
                            val xml = BufferedReader(it.reader(StandardCharsets.UTF_8))
                                .readText()
                                .replace("<link rel=\"stylesheet\" type=\"text/css\" href=\"report.css\">", "")

                            val parsedHtml = Jsoup.parse(xml.replace("'", "").replace("<br>", ""))

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
                                    propertyId to filing.extractedData?.find { it.propertyId == propertyId }?.value
                                }
                                reconciliationValues
                            }

//                            Cashflow(xml.byteInputStream(StandardCharsets.UTF_8)).extract()
                        }
                    } catch (e: Exception) {
                        e.printStackTrace()
                    }
                }
            }
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
