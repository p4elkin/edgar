package fi.avp.edgar

import fi.avp.edgar.util.forEachAsync
import fi.avp.edgar.util.runOnComputationThreadPool
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.bson.types.ObjectId
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import org.jsoup.parser.Parser
import org.litote.kmongo.and
import org.litote.kmongo.coroutine.CoroutineCollection
import org.litote.kmongo.eq
import java.lang.Exception
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.absoluteValue

@Serializable
data class CondensedReport(
    @Contextual
    var _id: ObjectId? = null,
    @Contextual
    val filingId: ObjectId,
    val dataUrl: String,
    val columns: List<Column> = emptyList(),
    val sections: List<Section> = emptyList(),
    val status: OperationStatus = OperationStatus.PENDING
)

@Serializable
data class Column(
    val id: String,
    val timespan: String,
    val label: String
)

@Serializable
data class Value(
    val value: String,
    val columnId: String
)

@Serializable
data class Section(
    val id: String,
    val label: String,
    val rows: MutableSet<Row> = hashSetOf()
)

@Serializable
data class Row(
    val propertyId: String,
    val label: String,
    val values: List<Value> = emptyList()
)

val props = setOf(
        "us-gaap:DepreciationAndAmortizationAbstract",
        "us-gaap:AdjustmentsForNoncashItemsIncludedInIncomeLossFromContinuingOperationsAbstract",
        "us-gaap:AdjustmentsToReconcileNetIncomeLossToCashProvidedByUsedInOperatingActivitiesAbstract",
        "us-gaap:AdjustmentsNoncashItemsToReconcileNetIncomeLossToCashProvidedByUsedInOperatingActivitiesAbstract",
        "us-gaap:AdjustmentsToReconcileIncomeLossToNetCashProvidedByUsedInContinuingOperationsAbstract",
        "fds:AdjustmentsToReconcileNetIncomeToNetCashProvidedByOperatingActivitiesAbstract",
        "logl:AdjustmentsToReconcileNetLossToCashFlowsFromOperatingActivitiesAbstract",
        "IncreaseDecreaseInOperatingCapitalAbstract",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperationsAbstract",
        "ReconciliationOfNetIncomeToNetCashProvidedByOperatingActivitiesAbstract")

fun main() {
    runBlocking {
        runOnComputationThreadPool {
//            parseCashflowStatements()
//            parseBalanceStatements()
            parseOperationsStatements()
        }
    }
}

val negativeEntryPattern = Regex("\\(.+\\)")
val cashIncomePattern = Regex("adjustment.*reconcile", RegexOption.IGNORE_CASE)
val cashIncomePatternAlt = Regex("reconciliation.*income", RegexOption.IGNORE_CASE)
val cashIncomePatternAlt2 = Regex("adjustments.*continuing", RegexOption.IGNORE_CASE)

suspend fun calculateReconciliationValues(filing: Filing): Double {
    Database.cashflow.ensureIndex(CondensedReport::filingId)
    return Database.cashflow.findOne(CondensedReport::filingId eq filing._id)?.let { condensedCashflowStatement ->
        val section = condensedCashflowStatement.sections.find {
            props.contains(it.id) ||
                    cashIncomePattern.containsMatchIn(it.id) ||
                    cashIncomePatternAlt.containsMatchIn(it.id) ||
                    cashIncomePatternAlt.containsMatchIn(it.id) ||
                    Regex(".*adjustments to reconcile.*").containsMatchIn(it.label)
        }

        val firstColumnId = condensedCashflowStatement.columns.firstOrNull()?.id
        if (section != null) {
            filing.extractedData?.let { extractedData ->
                val relatedPropertyList = section.rows
                    .mapNotNull {row ->
                        row.values.find { it.columnId == firstColumnId }?.let { valueFromCashflowStatement ->
                            if (valueFromCashflowStatement.value != "0" && !valueFromCashflowStatement.value.isBlank()) {
                                val value = extractedData.find {it.propertyId == row.propertyId.removePrefix("us-gaap:") }
                                if (value == null) {
                                    println("Failed to resolve ${row.propertyId} in ${condensedCashflowStatement.dataUrl}")
                                }

                                value?.numericValue()?.let {
                                    if (negativeEntryPattern.matches(valueFromCashflowStatement.value)) {
                                        -it.absoluteValue
                                    } else {
                                        it.absoluteValue
                                    }
                                }
                            } else null
                        }

                    }

                if (relatedPropertyList.contains(null)) {
                    null
                } else {
                    if (relatedPropertyList.sum().isNaN()) {
                        println("Result is not a number")
                    }

                    relatedPropertyList.sum()
                }
            }
        } else {
            println("${condensedCashflowStatement.dataUrl} No reconciliation data found ${condensedCashflowStatement.sections.map { it.id }}")
            Double.NEGATIVE_INFINITY
        }
    } ?: Double.NEGATIVE_INFINITY

}

suspend fun parseCashFlow(filing: Filing): CondensedReport? {
    return parseCondensedReport(filing, "${filing.dataUrl!!}/${filing.files!!.cashFlow}", filing.files?.cashFlow(filing.dataUrl!!))
}

suspend fun parseBalanceSheet(filing: Filing): CondensedReport? {
    return parseCondensedReport(filing, "${filing.dataUrl!!}/${filing.files!!.balance}", filing.files?.balance(filing.dataUrl!!))
}

suspend fun parseOperationsStatement(filing: Filing): CondensedReport? {
    return parseCondensedReport(filing, "${filing.dataUrl!!}/${filing.files!!.operations}", filing.files?.operations(filing.dataUrl!!))
}

suspend fun parseIncomeStatement(filing: Filing): CondensedReport? {
    return parseCondensedReport(filing, "${filing.dataUrl!!}/${filing.files!!.income}", filing.files?.income(filing.dataUrl!!))
}

fun parseCondensedReport(filing: Filing, fileUrl: String, condensedReportContent: String?): CondensedReport? {
    println("${counter.incrementAndGet()} ${filing.companyName} ${filing.dateFiled} ${filing.formType}")
    if (condensedReportContent == null) {
        println("content missing for ${fileUrl}")
    }
    return condensedReportContent?.let { condensedReport ->
        val xml = condensedReport.replace("<link rel=\"StyleSheet\" type=\"text/css\" href=\"report.css\">", "")
                .replace("'", "")
                .replace("<br>", "")

            val document = Jsoup.parse(xml, StandardCharsets.UTF_8.name(), Parser.xmlParser())
            XmlCondensedReport(document, filing, fileUrl).parse() ?: HtmlCondensedReport(document, filing, fileUrl).parse()
    }
}

class HtmlCondensedReport(val document: Document, val filing: Filing, val fileUrl: String) {

    fun parse(): CondensedReport {

//        println("parsing document: ${filing.dataUrl}/${filing.files!!.cashFlow}")
        val cols = resolveColumns()

        val defaultSection = Section("", "")
        val allSections = MutableList(1) { defaultSection }

        document.select("body > table > tr")
                //.drop(2) // - is it still needed?!
                .fold(allSections) { sections, row ->
                    resolvePropertyId(row)?.let { (propertyId, label) ->
                        val isSectionProperty = propertyId.endsWith("Abstract")

                        if (isSectionProperty) {
                            val newSection = Section(propertyId, label)
                            sections.add(newSection)
                        } else {
                            val cells = parseCells(row, propertyId, label)
                            if (cells.values.size != cols.size) {
                                println("Suspicious document: $fileUrl")
                            }
                            sections.last().rows.add(cells)
                        }
                    }

                    sections
                }

        return CondensedReport(
                columns = cols,
                sections = allSections,
                filingId = filing._id!!,
                dataUrl = fileUrl,
                status = OperationStatus.DONE)
    }

    private fun resolvePropertyId(row: Element): Pair<String, String>? {
        return row.selectFirst("td > a[onclick]")?.let { cellWithPropertyId ->
            cellWithPropertyId.attr("onclick").let {
                val propertyId = Regex("defref_(.+),")
                        .find(it)
                        ?.groups
                        ?.get(1)
                        ?.value
                propertyId?.let {pid ->
                    pid.replace("_", ":") to cellWithPropertyId.text()
                }
            }
        }
    }

    private fun resolveColumns(): List<Column> {
        val headerRowSpan =
                document.select("tr:eq(0)")
                        .select("th:eq(0)")?.attr("rowspan")?.toInt() ?: 0

//        val reportTitleCell = document.select("tr:eq(0) > th:eq(0")
        if (headerRowSpan > 1) {
            val timeSpanHeaders = document.select("th[colspan]:gt(0)")
            val spans: List<String> = timeSpanHeaders.fold(emptyList()) { result, td ->
                val colSpan = td.attr("colspan").toInt()
                val spanText = td.text()
                result.plus((0 until colSpan).map { spanText }.toList())
            }

            return document.select("tr:eq(1) > th")
                    //.drop(2) // - is it still needed?!
                    .filter { it.select("sup").size == 0 }
                    .mapIndexed { index, th ->
                        Column(
                                id = "$index",
                                label = th.select("div").text(),
                                timespan = spans[index])
                    }
        } else {
            return document.select("tr:eq(0)")
                    .select("th:gt(0)")
                    .filter { it.select("sup").size == 0 }
                    .mapIndexed { index, th ->
                        Column(id = "$index",
                               label = th.select("div").text(),
                               timespan = "")
                    }
        }
    }

    private fun parseCells(row: Element, propertyId: String, label: String): Row {
        // skip first cell (caption)
        val values = row.select("td:gt(0)")
                .filter { it.attr("class") != "fn" }
                .filter { it.select("sup").size == 0 }
                // skip property id
                .mapIndexed { index, cell ->
                    // if text element is present - take it, otherwise - find first actually rendered child
                    val rawText = cell.textNodes().firstOrNull()?.text() ?: cell.children().first { !it.hasAttr("display")}?.text()
                    val text = rawText
                            ?.trim('$')
                            ?.trim(' ') ?: ""

                    Value(value = text,
                          columnId = index.toString())
                }.toList()

        return Row(propertyId, label, values)
    }
}

class XmlCondensedReport(val document: Document, val filing: Filing, val fileUrl: String) {

    fun parse(): CondensedReport? {
        val isXmlBasedCashFlowReport = filing.files?.cashFlow?.endsWith("xml") ?: false

        return if (isXmlBasedCashFlowReport) {

            val columns = resolveColumns()
            val defaultSection = Section("", "")
            val allSections = MutableList(1) { defaultSection }

            document.select("Rows > Row")
                    .fold(allSections) { sections, row ->
                        val isSectionRow = row.select("IsAbstractGroupTitle").text().toBoolean()
                        val (propertyId, label) = resolvePropertyId(row)

                        if (!isSectionRow) {
                            val values = row.select("Cells").select("Cell").map { cell ->
                                Value(
                                        value = cell.select("RoundedNumericAmount").text(),
                                        columnId = cell.select("Id").text()
                                )
                            }.toList()

                            sections.last().rows.add(Row(propertyId, label, values))
                        } else {
                            sections.add(Section(propertyId, label))
                        }

                        sections
                    }

            CondensedReport(
                columns = columns,
                sections = allSections,
                filingId = filing._id!!,
                dataUrl = "${filing.dataUrl!!}/${filing.files!!.cashFlow!!}",
                status = OperationStatus.DONE
            )
        } else {
            null
        }
    }

    private fun resolveColumns(): List<Column> {
        val columnsNode = document.select("Columns")
        val columns = columnsNode.select("Column").toList()
        return columns.map {
            val labels = it.select("Labels > Label").map { it.attr("Label") }.toList()
            val timespan = if (labels.size > 1) labels[0] else ""
            val label = if (labels.size > 1) labels[1] else labels[0]

            Column(
                    id = it.selectFirst("Id").text(),
                    label = label,
                    timespan = timespan)
        }.toList()
    }

    private fun resolvePropertyId(row: Element): Pair<String, String> {
        return row.select("ElementName").text().replace("_", ":") to
               row.select("Label").text()
    }
}

suspend fun parseIncomeStatements() {
    parse(Database.income) { parseIncomeStatement(it) }
}

suspend fun parseOperationsStatements() {
    parse(Database.operations) { parseOperationsStatement(it) }
}

suspend fun parseBalanceStatements() {
    parse(Database.balance) { parseBalanceSheet(it) }
}

suspend fun parseCashflowStatements() {
    parse(Database.cashflow) { parseCashFlow(it) }
}

suspend fun parse(collection: CoroutineCollection<CondensedReport>, function: suspend (Filing) -> CondensedReport?) {
    collection.drop()
    collection.ensureIndex("{dataUrl: 1}")
    val counter = AtomicInteger(0)
    Database.getAllFilings(
        and(
            Filing::formType eq "10-K"
            //, Filing::dateFiled gt LocalDate.now().minusDays(50)
        ))
        .forEachAsync { cursor ->
            cursor.consumeEach {filing ->
                coroutineScope {
                    try {
                        function(filing)?.let {
                            collection.save(it)
                            println("saved ${counter.incrementAndGet()}")
                        }
                    }  catch (e: Exception) {
                        println("failed to parse condensed report")
                        collection.save(CondensedReport(
                            filingId = filing._id!!,
                            dataUrl = "",
                            status = OperationStatus.FAILED))
                    }
                }
            }
        }
}
