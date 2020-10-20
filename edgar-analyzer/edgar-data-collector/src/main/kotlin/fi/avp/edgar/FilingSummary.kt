package fi.avp.edgar

import fi.avp.edgar.util.find
import fi.avp.edgar.util.list
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
        return findFilingNode("of income")
    }

    fun getConsolidatedBalanceSheet(): String? {
        return findFilingNode("balance sheet")
    }

    fun getConsolidatedStatementOfOperation(): String? {
        return findFilingNode("of operation")
    }

    fun getConsolidatedStatementOfCashFlow(): String? {
        return findFilingNode("cash flow")
    }

    fun getFinancialSummary(): String? {
        return findFilingNode("financial summary")
    }

    private fun findFilingNode(title: String): String? {
        val filingNode = reports?.list()?.find {
            it.find("ShortName")?.textContent?.toLowerCase()?.contains(title) ?: false
        }

        return (filingNode?.find("XmlFileName") ?: filingNode?.find("HtmlFileName"))?.textContent
    }
}
