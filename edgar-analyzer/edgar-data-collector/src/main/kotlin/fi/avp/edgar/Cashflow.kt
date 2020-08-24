package fi.avp.edgar

import kotlinx.coroutines.runBlocking
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.jsoup.parser.Parser
import org.w3c.dom.Document
import java.io.BufferedReader
import java.io.InputStream
import java.nio.charset.StandardCharsets
import javax.xml.crypto.Data
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPath
import javax.xml.xpath.XPathFactory
import kotlin.system.measureTimeMillis

val propertyPattern = Regex(".*_(.+),")

data class Column(
    val id: String,
    val labels: Set<String>
)

data class Value(
    val value: String,
    val columnId: String
)

data class Section(
    val id: String,
    val rows: MutableSet<Row> = hashSetOf()
)

data class Row(
    val propertyId: String,
    val label: String,
    val values: List<Value> = emptyList()
)

fun main() {
    runBlocking {
        Database.filings.find("{ticker: 'AAPL'}").consumeEach {
            it.files?.cashFlow(it.dataUrl!!)?.let { cashflow ->
                val xml = cashflow.replace("<link rel=\"StyleSheet\" type=\"text/css\" href=\"report.css\">", "")
                    .replace("'", "")
                    .replace("<br>", "")

                try {
                    println(it.files?.xbrlReport)
                    val document = Jsoup.parse(xml, StandardCharsets.UTF_8.name(), Parser.xmlParser())
                    val columnsNode = document.select("Columns")
                    if (columnsNode.size > 0) {
                        val columns = columnsNode.select("Column").toList()
                        val parsedColumnInfo = columns.map {
                            Column(
                                id = it.selectFirst("Id").text(),
                                labels = it
                                    .select("Labels")
                                    .select("Label")
                                    .map {
                                        it.attr("Label")
                                    }.toSet())
                        }.toList()

                        val sections = ArrayList<Section>()
                        var currentSection: Section? = null
                        document.select("Rows").select("Row").map { row ->
                            val propertyId = row.select("ElementName").text().replace("_", ":")
                            val isSectionRow = row.select("IsAbstractGroupTitle").text().toBoolean()
                            val label = row.select("Label").text()

                            if (isSectionRow) {
                                val newSection = Section(propertyId)
                                sections.add(newSection)
                                currentSection = newSection
                            } else {
                                if (currentSection == null) {
                                    val defaultSection = Section("")
                                    sections.add(defaultSection)
                                    currentSection = defaultSection
                                }

                                val values = row.select("Cells").select("Cell").map { cell ->
                                    Value(
                                        value = cell.select("RoundedNumericAmount").text(),
                                        columnId = cell.select("Id").text())
                                }.toList()

                                currentSection!!.rows.add(Row(propertyId, label, values))
                            }
                        }

                        println(sections)
                    } else {
                        val rows = document.select("tr")
                            .map {

                            }

                    }

//                    println(parsed.extract())
                } catch (e: Exception) {

                }
            }
        }
    }
}

suspend fun calcNonPaperCashflow() {
    Database.getSP500Companies().forEach {
        val companyInfo = it
        val doneIn = measureTimeMillis {
            val filings =
                it.cik.flatMap {
                    runBlocking {
                        Database.getFilingsByCik(it.toString())
                    }
                }.filter { it.formType == "10-K" }

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
