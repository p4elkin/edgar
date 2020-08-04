package fi.avp.edgar.mining

import fi.avp.util.find
import fi.avp.util.list
import org.w3c.dom.Document
import org.w3c.dom.NodeList
import java.io.InputStream
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPath
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathFactory

open class FilingSummary(private val summaryStream: InputStream) {

    private val xpath: XPath by lazy {
        XPathFactory.newInstance().newXPath()
    }

    private val filingSummary: Document by lazy {
        try {
            val builderFactory = DocumentBuilderFactory.newInstance()
            builderFactory.isNamespaceAware = true

            val builder = builderFactory.newDocumentBuilder()
            builder.parse(summaryStream)
        } catch (e: Exception) {
            throw e
        } finally {
            summaryStream.close()
        }
    }

    private val reports: NodeList? by lazy {
        xpath
            .compile("//Report")
            .evaluate(filingSummary, XPathConstants.NODESET) as NodeList?
    }

    fun getConsolidatedStatementOfIncome(): String? {
        return findReportRefNode("of income") ?: findReportRefNode("")
    }

    fun getConsolidatedBalanceSheet(): String? {
        return findReportRefNode("balance sheet")
    }

    fun getConsolidatedStatementOfOperation(): String? {
        return findReportRefNode("of operation")
    }

    fun getConsolidatedStatementOfCashFlow(): String? {
        return findReportRefNode("cash flow")
    }

    fun getFinancialSummary(): String? {
        return findReportRefNode("financial summary")
    }

    private fun findReportRefNode(title: String): String? {
        val reportRefNode = reports?.list()?.find {
            it.find("ShortName")?.textContent?.toLowerCase()?.contains(title) ?: false
        }

        return (reportRefNode?.find("XmlFileName") ?: reportRefNode?.find("HtmlFileName"))?.textContent
    }
}
