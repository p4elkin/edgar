package fi.avp.edgar

import com.github.michaelbull.retry.retry
import fi.avp.edgar.util.forEachAsync
import fi.avp.edgar.util.runOnComputationThreadPool
import fi.avp.edgar.util.forEachAsync
import fi.avp.edgar.util.runOnComputationThreadPool
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.bson.types.ObjectId
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.jsoup.parser.Parser
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
    "ReconciliationOfNetIncomeToNetCashProvidedByOperatingActivitiesAbstract")

fun main() {
    runBlocking {
        runOnComputationThreadPool {
            parseCashflowStatements()
        }
    }
}

val negativeEntryPattern = Regex("\\(.+\\)")
val cashIncomePattern = Regex("adjustment.*reconcile", RegexOption.IGNORE_CASE)
val cashIncomePatternAlt = Regex("reconciliation.*income", RegexOption.IGNORE_CASE)
val cashIncomePatternAlt2 = Regex("adjustments.*continuing", RegexOption.IGNORE_CASE)

suspend fun calculateReconciliationValues(filing: Filing): Double? {
    Database.cashflow.ensureIndex(CondensedReport::filingId)
    return Database.cashflow.findOne(CondensedReport::filingId eq filing._id)?.let { condensedCashflowStatement ->
        val section = condensedCashflowStatement.sections.find {
            props.contains(it.id) ||
            cashIncomePattern.containsMatchIn(it.id) ||
            cashIncomePatternAlt.containsMatchIn(it.id) ||
            cashIncomePatternAlt.containsMatchIn(it.id)
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
            null
        }
    }
}

fun resolvePropertyInformation(row: Element): Pair<String, String>? {
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

suspend fun parseCashflowStatement(filing: Filing) {
    println(counter.incrementAndGet())
    try {
        retry {
            filing.files?.cashFlow(filing.dataUrl!!)?.let { cashflow ->
                val dataUrl = "${filing.dataUrl!!}/${filing.files?.cashFlow}"
////
//                            if (Database.cashflow.findOne("{dataUrl: '${dataUrl}'}") != null) {
//                                println("${dataUrl} is already parsed")
//                                return@let
//                            }

                val sections = ArrayList<Section>()
                var currentSection: Section? = null

                val xml = cashflow.replace("<link rel=\"StyleSheet\" type=\"text/css\" href=\"report.css\">", "")
                        .replace("'", "")
                        .replace("<br>", "")


                val updateCurrentSection: (String, String, Boolean) -> Unit? = { propertyId, label, isSectionProperty ->
                    val newSection = Section(propertyId, label)
                    if (isSectionProperty) {
                        sections.add(newSection)
                        currentSection = newSection
                    } else {
                        if (currentSection == null) {
                            val defaultSection = Section("", "")
                            sections.add(defaultSection)
                            currentSection = defaultSection
                        }
                    }
                }

                try {
                    val document = Jsoup.parse(xml, StandardCharsets.UTF_8.name(), Parser.xmlParser())
                    val columnsNode = document.select("Columns")
                    val report = if (columnsNode.size > 0) {
                        val columns = columnsNode.select("Column").toList()
                        val parsedColumnInfo = columns.map {
                            val labels = it.select("Labels > Label").map { it.attr("Label") }.toList()
                            val timespan = if (labels.size > 1) labels[0] else ""
                            val label = if (labels.size > 1) labels[1] else labels[0]
                            Column(
                                    id = it.selectFirst("Id").text(),
                                    label = label,
                                    timespan = timespan)
                        }.toList()

                        document.select("Rows").select("Row").map { row ->
                            val propertyId = row.select("ElementName").text().replace("_", ":")
                            val isSectionRow = row.select("IsAbstractGroupTitle").text().toBoolean()
                            val label = row.select("Label").text()

                            updateCurrentSection(propertyId, label, isSectionRow)

                            if (!isSectionRow) {
                                val values = row.select("Cells").select("Cell").map { cell ->
                                    Value(
                                            value = cell.select("RoundedNumericAmount").text(),
                                            columnId = cell.select("Id").text())
                                }.toList()

                                currentSection!!.rows.add(Row(propertyId, label, values))
                            }
                        }

                        CondensedReport(columns = parsedColumnInfo, sections = sections, filingId = filing._id!!, dataUrl = dataUrl, status = OperationStatus.DONE)
                    } else {

                        val headers = document.select("th[colspan]:gt(0)")
                        val spans: List<String> = headers.fold(emptyList()) { result, td ->
                            val colSpan = td.attr("colspan").toInt()
                            val spanText = td.text()
                            result.plus((0 until colSpan).map { spanText }.toList())
                        }

                        val cols = document.select("tr:eq(1) > th")
                                .mapIndexed { index, th ->
                                    Column(
                                            id = "$index",
                                            label = th.select("div").text(),
                                            timespan = spans[index])
                                }

                        document.select("body > table > tr")
                                .drop(2)
                                .forEach { row ->
                                    val propertyInformation = resolvePropertyInformation(row)
                                    if (propertyInformation == null) {
                                        sections.add(Section(
                                                id = row.selectFirst("td").text(),
                                                label = row.selectFirst("td").text(),
                                        ))
                                    } else {
                                        propertyInformation.let { (propertyId, label) ->
                                            val isSectionProperty = propertyId.endsWith("Abstract")
                                            updateCurrentSection(propertyId, label, isSectionProperty)

                                            if (!isSectionProperty) {
                                                val values = row.select("td:gt(0)")
                                                        .filter { it.attr("class") != "fn"}
                                                        // skip property id
                                                        .mapIndexed { index, cell ->
                                                            Value(
                                                                    value = cell.text(),
                                                                    columnId = index.toString())
                                                        }.toList()

                                                currentSection!!.rows.add(Row(propertyId, label, values))
                                            }
                                        }
                                    }
                                }


                        CondensedReport(
                                columns = cols,
                                sections = sections,
                                filingId = filing._id!!,
                                dataUrl = dataUrl,
                                status = OperationStatus.DONE)
                    }

                    Database.cashflow.save(report)
                } catch (e: Exception) {
                    Database.cashflow.save(CondensedReport(
                            filingId = filing._id!!,
                            dataUrl = dataUrl,
                            status = OperationStatus.FAILED))
                    println("failed to parse ${dataUrl} due to ${e.message}")
                }
            }
        }
    } catch (e: Exception) {
        println("failed to parse open cashflow file")
        Database.cashflow.save(CondensedReport(
                filingId = filing._id!!,
                dataUrl = "",
                status = OperationStatus.FAILED))
    }
}

suspend fun parseCashflowStatements() {
    Database.cashflow.ensureIndex("{dataUrl: 1}")
    val counter = AtomicInteger(0)
    Database.getAllFilings("{formType: '10-K'}").forEachAsync { cursor ->
        cursor.consumeEach {filing ->
            coroutineScope {
                parseCashflowStatement(filing)
            }
        }
    }
}
