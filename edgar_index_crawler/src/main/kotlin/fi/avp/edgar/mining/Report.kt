package fi.avp.edgar.mining

import fi.avp.edgar.data.PropertyDescriptor
import fi.avp.edgar.data.attrNames
import fi.avp.util.attr
import fi.avp.util.find
import fi.avp.util.list
import org.w3c.dom.Document
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import java.io.InputStream
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeParseException
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPath
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathFactory
import kotlin.math.abs
import kotlin.math.pow

data class ReportDataExtractionResult(
    val data: List<PropertyData>,
    val problems: ReportProblems
)

data class ReportProblems(val suspiciousContexts: Set<String>, val missingProperties: Set<PropertyDescriptor>) {
    override fun toString(): String {
        val ctx = suspiciousContexts.map { "$it" }.joinToString()
        val prop = missingProperties.map { it.id }.joinToString()
        return "$ctx and $prop"
    }
}

data class PropertyData(
    val descriptor: PropertyDescriptor,
    val extractedValues: List<ExtractedValue>,
    val contexts: Set<Context>,
    val valueUnits: Set<ValueUnit>?
)

data class ExtractedValue (
    val propertyId: String,
    val value: String,
    val decimals: String,
    // power of 10
    val scale: String,
    val context: String,
    val unit: String?,
    // value difference since last report
    val delta: Double? = 0.0) {

    fun numericValue(): Double {
        return try {
            val decimals = try {
                decimals.toDouble()
            } catch(e: Exception) {
                0.0
            }

            parseValue(value, decimals)
        } catch(e: Exception) {
            Double.NaN
        }
    }
}

fun parseValue(value: String, decimals: Double): Double {
    val withoutSeparator: Double = value.replace(",", "").replace(".", "").toDouble()
    val ratio = 10.0.pow(-decimals)

    return if (withoutSeparator % ratio != 0.0) {
        withoutSeparator * ratio
    } else {
        withoutSeparator
    }
}

data class ValueUnit(
    val id: String,
    val measure: String?,
    val divide: Pair<String, String>?)

data class Period(val startDate: LocalDate, val endDate: LocalDate, val isInstant: Boolean = false) {
    val duration: Long
        get() = Duration.between(startDate.atStartOfDay(), endDate.atStartOfDay()).toDays()
}

data class Context(val id: String, val period: Period?, val segment: String? = null)

open class Report(val content: InputStream, private val reportType: String, private val isInline: Boolean) {

    private val contextCache = hashMapOf<String, Context?>()
    private val unitCache = hashMapOf<String, ValueUnit?>()

    private val xpath: XPath by lazy {
            XPathFactory.newInstance().newXPath()
        }

    private val reportDoc: Document by lazy {
            val builderFactory = DocumentBuilderFactory.newInstance()
            builderFactory.isNamespaceAware = true
            builderFactory.newDocumentBuilder().parse(content)
        }

    fun extractData(date: LocalDateTime): ReportDataExtractionResult {
        // after resolving some basic properties - detect
        fun getNonAmbiguousContexts(props: List<PropertyData>): Set<String> {
            return props.filter {
                it.extractedValues
                    .groupBy { it.propertyId }
                    .all { it.value.size == 1 }
            }.flatMap {
                it.extractedValues.map { it.context }
            }.toSet()
        }

        fun resolveProperty(propertyDescriptor: PropertyDescriptor): PropertyData {

            fun filterByReportPeriodProximity(nodes: List<ExtractedValue>): List<ExtractedValue> {
                val closestToReportDate =  nodes.groupBy {
                    val endDate= contextById(it.context)?.period?.endDate?.atStartOfDay() ?: date;
                    Duration.between(endDate, date)
                }.minBy {
                    it.key
                }?.value ?: emptyList()

                if (closestToReportDate.size == 1) {
                    return closestToReportDate
                }

                return closestToReportDate.groupBy {
                    val duration = contextById(it.context)?.period?.duration ?: 0
                    if (reportType == "10-Q") kotlin.math.abs(90 - duration) else kotlin.math.abs(365 - duration)
                }.minBy {
                    it.key
                }?.value ?: emptyList()
            }

            // Fetch all the variants
            val resolvedVariants = propertyDescriptor.variants
                .flatMap { variantId ->
                    resolveNodes(variantId).list().map { extractValue(it, variantId) }
                }

            val actualisedMetrics = filterByReportPeriodProximity(resolvedVariants)
            return PropertyData(propertyDescriptor, actualisedMetrics,
                actualisedMetrics
                    .map { contextById(it.context)!! }
                    .toSet(),

                actualisedMetrics
                    .map { it.unit }
                    .filterNotNull()
                    .map { unitById(it)!! }
                    .toSet()
            )
        }

        val resolvedBasicProperties = attrNames.map { resolveProperty(it) }.plus(resolveDei())

        val allContexts = resolvedBasicProperties
            .flatMap { it.contexts }
            .map { it.id to it }
            .toMap()

        val nonAmbiguousContexts = getNonAmbiguousContexts(resolvedBasicProperties)
        val disambiguatedBasicProperties = disambiguateProperties(nonAmbiguousContexts, allContexts.values.toSet(), resolvedBasicProperties)
        val relevantContexts = disambiguatedBasicProperties.flatMap {
            it.extractedValues.map { contextById(it.context)!! }
        }.toSet()

        val problems = sanitiseExtractedData(relevantContexts, reportType, disambiguatedBasicProperties)
        val data = relevantContexts.filter { it.id !in problems.suspiciousContexts }.flatMap {
            extractAllPropertiesForContext(it.id)
        }.map {
            PropertyData(
                PropertyDescriptor(variants = listOf(it.propertyId), id = it.propertyId, category = "misc"),
                extractedValues = listOf(it),
                contexts = setOf(allContexts[it.context]!!),
                valueUnits = it.unit?.let { setOf(unitById(it)!!) })
        }

        return ReportDataExtractionResult(data, problems)
    }

    private fun getPropertyName(node: Node): String {
       val pName =  if (isInline) {
           node.attr("name")!!
       } else {
           node.nodeName
       }

       return if (pName.startsWith("us-gaap")) pName.substringAfter("us-gaap:") else pName
    }

    private fun extractAllPropertiesForContext(contextId: String): List<ExtractedValue> {
        return resolveNodesReferencingContext(contextId).list().map {
            ExtractedValue(
                propertyId = getPropertyName(it),
                value = it.textContent,
                decimals = it.attr("decimals") ?: "",
                scale = it.attr("scale") ?: "",
                context = contextId,
                unit = it.attr("unitRef")
            )
        }
    }

    private fun resolveDei(): List<PropertyData> {
        val deiNodes = xpath.compile("//*[starts-with(name(), 'dei:')]")
            .evaluate(this.reportDoc, XPathConstants.NODESET) as NodeList

        return deiNodes.list()
            .map { extractValue(it, it.nodeName) }
            .map {
                PropertyData(
                    PropertyDescriptor(
                        variants = listOf(it.propertyId), id = it.propertyId, category = "dei"
                    ),
                    extractedValues = listOf(it),
                    contexts = setOf(contextById(it.context)!!),
                    valueUnits = it.unit?.let { setOf(unitById(it)!!) })
            }
    }

    private fun extractValue(node: Node, propertyId: String): ExtractedValue {
        // we always assume that ctx is there
        val ctx = contextById(node.attr("contextRef")?.trim()!!)

        //
        val unit = node.attr("unitRef")?.let {
            unitById(it)
        }

        if (ctx == null) {
            throw RuntimeException()
        }

        return ExtractedValue(
            propertyId, node.textContent, context = ctx.id, unit = unit?.id,
            scale = node.attr("scale") ?: "", decimals = node.attr("decimals") ?: ""
        )
    }

    private fun resolveNodesReferencingContext(contextId: String): NodeList {
        val selector = "//*[@contextRef='${contextId}']"
        return xpath.compile(selector).evaluate(reportDoc, XPathConstants.NODESET) as NodeList
    }

    private fun resolveNodes(attrId: String): NodeList {
        val idWithoutNamespace = attrId.substringAfter(":")
        val selector = if (isInline)
            "//*[@name ='us-gaap:$idWithoutNamespace']"
        else
            "//*[local-name() = '$idWithoutNamespace']"

        return xpath.compile(selector).evaluate(reportDoc, XPathConstants.NODESET) as NodeList
    }

    private fun unitById(id: String): ValueUnit? {
        return unitCache.computeIfAbsent(id) {
            singleNode("//*[local-name()='unit' and @id='$id']")?.let {
                ValueUnit(
                    id,
                    it.find("measure")?.textContent,
                    it.find("divide")?.let {
                        it.find("unitNumerator")?.find("measure")?.textContent!! to
                                it.find("unitDenominator")?.find("measure")?.textContent!!
                    })
            }
        }
    }

    private fun contextById(id: String): Context? {
        return contextCache.computeIfAbsent(id) {
            singleNode("//*[local-name()='context' and @id='$id']")?.let {
                val periodNode = it.find("period")
                val segment = it.find("entity")?.find("segment")?.textContent

                val period = periodNode?.let {
                    val instant = it.find("instant")
                    val startDate = it.find("startDate") ?: instant
                    val endDate = it.find("endDate") ?: instant

                    try {
                        val startDateParsed = LocalDate.parse(startDate?.textContent?.trim())
                        val endDateParsed = LocalDate.parse(endDate?.textContent?.trim())

                        Period(startDateParsed, endDateParsed, instant != null)
                    } catch (e: DateTimeParseException) {
                        e.printStackTrace()
                        null
                    }
                }

                Context(id, period, segment)
            }
        }
    }

    private fun nodeSet(selector: String): NodeList {
        return xpath.compile(selector).evaluate(reportDoc, XPathConstants.NODESET) as NodeList
    }


    private fun singleNode(selector: String): Node? {
        return xpath.compile(selector).evaluate(reportDoc, XPathConstants.NODE) as Node?
    }

    private fun disambiguateProperties(nonAmbiguousContexts: Set<String>, allContexts: Set<Context>, data: List<PropertyData>): List<PropertyData> {
        return data.map {
            val contexts = it.extractedValues
                .map { it.context }
                .map { contextId -> allContexts.find { it.id == contextId }!! }
                .toSet()

            // first prefer nonAm
            var contextSet = contexts
                .filter {nonAmbiguousContexts.contains(it.id) }.toSet()

            if (contextSet.isEmpty()) {
                contextSet = contexts.filter { it.segment == null }.toSet()
            }

            if (contextSet.isEmpty()) {
                it
            } else {
                val segmentLessContextIds = contextSet.map { it.id }
                val filteredValues = it.extractedValues.filter {
                    it.context in segmentLessContextIds
                }

                it.copy(
                    contexts = contextSet,
                    extractedValues = filteredValues
                )
            }
        }
    }

    private fun sanitiseExtractedData(contexts: Set<Context>, reportType: String, resolvedData: List<PropertyData>): ReportProblems {
        val targetPeriod = if (reportType == "10-Q") 90 else 365
        val suspiciousContexts = contexts
            .filter {
                it.period == null || (!it.period.isInstant && abs(it.period.duration - targetPeriod) > 21)
            }
            .map { it.id }
            .toSet()

        val missingProperties = attrNames.filter { propertyDescriptor ->
            resolvedData
                .filter { propertyData -> propertyData.extractedValues.any { it.context !in suspiciousContexts } }
                .none { it.descriptor == propertyDescriptor }
        }.toSet()

        return ReportProblems(
            suspiciousContexts = suspiciousContexts,
            missingProperties = missingProperties
        )
    }


}
