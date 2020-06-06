package fi.avp.edgar

import fi.avp.edgar.data.ReportRecord
import fi.avp.util.Locations
import fi.avp.util.companyQuarterlyReport
import fi.avp.util.find
import fi.avp.util.preparePerCompanyReportStorageStructure
import org.w3c.dom.Document
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import java.io.InputStream
import java.nio.file.Files
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneOffset
import javax.xml.namespace.NamespaceContext
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathFactory

data class ReportData(val assets: Int)

fun main(args: Array<String>) {

    val reports = preparePerCompanyReportStorageStructure();
    reports.filterKeys { it.name == "apple_inc" }.forEach { (company, annualData) ->
        annualData.forEach { (year, reports)  ->
            reports.values.flatten()
                .sortedBy { it.date.toInstant(ZoneOffset.UTC).toEpochMilli() }
                .filter { it.reportType == "10-K" }
                .forEach { reportRecord ->
                    val companyQuarterlyReport = companyQuarterlyReport(reportRecord)
                    companyQuarterlyReport?.let {
                        val isInlineXbrl = it.fileName.toString().endsWith(".htm")
                        XbrlParser(Files.newInputStream(it), reportRecord, isInlineXbrl).parseReport(reportRecord)
                    }
            }
        }
    }
}

class XbrlParser(val content: InputStream, val reportRecord: ReportRecord, val isInlineXbrl: Boolean = false) {

    private val xpath = XPathFactory.newInstance().newXPath();

    private val parsedXml: Document by lazy {
        val documentBuilderFactory = DocumentBuilderFactory.newInstance()
        documentBuilderFactory.isNamespaceAware = true
        documentBuilderFactory.newDocumentBuilder().parse(content)

    }

    private fun resolveAttrNodes(attrId: String): NodeList {
        val selector = if (isInlineXbrl) "//*[@name ='$attrId']" else "//$attrId"
        return xpath.compile(selector).evaluate(parsedXml, XPathConstants.NODESET) as NodeList
    }

    fun parseReport(reportRecord: ReportRecord) {
        val netCashBlaBla = resolveAttrNodes("us-gaap:NetCashProvidedByUsedInOperatingActivities");
        val totalAssets =
//            attributeValuesToContext("us-gaap:Assets")
            attributeValuesToContext("us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax")
                .filter { it.second?.segment == null }
                .groupBy {(_, context) ->
                    Duration.between(context?.endDate?.atStartOfDay() ?: LocalDate.MAX.atStartOfDay(), reportRecord.date)
                }.minBy { it.key }
        println(totalAssets?.value)
//        printAll("Total shareholders’ equity", resolveAttr("us-gaap:StockholdersEquity"))
//        printAll("Net income", resolveAttr("us-gaap:NetIncomeLoss"))
//        printAll("Total net sales", resolveAttr("us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"))
//        printAll("netCash", netCashBlaBla)
//        printAll("Cash generated by/(used in) investing activities", resolveAttr("us-gaap:NetCashProvidedByUsedInInvestingActivities"))
    }

    data class Context(val id: String, val startDate: LocalDate, val endDate: LocalDate, val isInstant: Boolean = false, val segment: String? = null) {
        val duration: Long
            get() = java.time.Duration.between(startDate.atStartOfDay(), endDate.atStartOfDay()).toDays()
    }


    private fun attributeValuesToContext(attrId: String): List<Pair<String, Context?>> {
        val nodes: NodeList = resolveAttrNodes(attrId)
        return List(nodes.length) {
            val contextId = nodes.item(it).attributes.getNamedItem("contextRef").textContent
            nodes.item(it).textContent to contextById(contextId)
        }
    }

    private fun contextById(id: String): Context? {
        val ctxNode = xpath.compile("//*[local-name()='context' and @id='$id']").evaluate(parsedXml, XPathConstants.NODE) as Node?
        val period = ctxNode?.find("period")
        val segment = ctxNode?.find("entity")?.find("segment")?.textContent

        return period?.let {
            val instant = period.find("instant")
            val startDate = period.find("startDate") ?: instant
            val endDate = period.find("endDate") ?: instant

            Context(id, LocalDate.parse(startDate?.textContent), LocalDate.parse(endDate?.textContent), instant != null, segment)
        }
    }
}
