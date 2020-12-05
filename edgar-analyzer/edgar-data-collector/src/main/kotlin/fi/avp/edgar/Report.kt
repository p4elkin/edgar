package fi.avp.edgar

import fi.avp.edgar.util.*
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
import javax.xml.xpath.XPathExpression
import javax.xml.xpath.XPathFactory
import kotlin.math.abs
import kotlin.text.find

data class ReportDataExtractionResult(
    val data: List<PropertyData>,
    val problems: ReportProblems
)

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
        // after resolving some basic properties - detect those that are specified only in one context
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

            // between several extracted values choose one that is closest to report date
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
                }.minByOrNull {
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

                actualisedMetrics.mapNotNull
                    { it.unit }
                    .map { unitById(it)!! }
                    .toSet()
            )
        }

        val resolvedBasicProperties = attrNames.map { resolveProperty(it) }

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
        val props = relevantContexts.filter { it.id !in problems.suspiciousContexts }.flatMap {
            extractAllPropertiesForContext(it.id)
        }

        val data = props.map {
            PropertyData(
                PropertyDescriptor(variants = listOf(it.propertyId), id = it.propertyId, category = "misc"),
                extractedValues = listOf(it),
                contexts = setOf(allContexts[it.context]!!),
                valueUnits = it.unit?.let { setOf(unitById(it)!!) })
        }.plus(resolveDei())

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
                isNegative = it.attr("sign")?.equals("-") ?: false,
                context = contextId,
                unit = it.attr("unitRef")
            )
        }
    }

    private fun resolveDei(): List<PropertyData> {
        val deiNodes = if (isInline)
        xpathExpression("//*[starts-with(@name, 'dei:')]")
                .evaluate(this.reportDoc, XPathConstants.NODESET) as NodeList
        else
            xpathExpression("//*[starts-with(name(), 'dei:')]")
                .evaluate(this.reportDoc, XPathConstants.NODESET) as NodeList

        return deiNodes.list()
            .map { extractValue(it, if (isInline) it.attr("name")!! else it.nodeName) }
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
            println("Failed to resolve context for ${node.nodeName}, attrs are: ${node.attrList().map { it }.joinToString()}")
        }

        return ExtractedValue(
            propertyId, node.textContent,
            context = ctx?.id ?: "",
            unit = unit?.id,
            scale = node.attr("scale") ?: "",
            isNegative = node.attr("sign")?.equals("-") ?: false,
            decimals = node.attr("decimals") ?: ""
        )
    }

    private val compiledXPathCache: MutableMap<String, XPathExpression> = hashMapOf()

    private fun xpathExpression(selector: String): XPathExpression {
        return compiledXPathCache.computeIfAbsent(selector) { xpath.compile(it) }
    }

    private fun resolveNodesReferencingContext(contextId: String): NodeList {
        val selector = "//*[@contextRef='${contextId}']"
        return xpathExpression(selector).evaluate(reportDoc, XPathConstants.NODESET) as NodeList
    }

    private fun resolveNodes(attrId: String): NodeList {
        val idWithoutNamespace = attrId.substringAfter(":")
        val selector = if (isInline)
            "//*[@name ='us-gaap:$idWithoutNamespace']"
        else
            "//*[local-name() = '$idWithoutNamespace']"

        return xpathExpression(selector).evaluate(reportDoc, XPathConstants.NODESET) as NodeList
    }

    private fun unitById(id: String): ValueUnit? {
        return unitCache.computeIfAbsent(id) {
            xpathExpression("//*[local-name()='unit' and @id='$id']")
                .singleNode(reportDoc)?.let {
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
            xpathExpression("//*[local-name()='context' and @id='$id']").singleNode(reportDoc)?.let {
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
                    extractedValues = filteredValues)
            }
        }
    }

    private fun sanitiseExtractedData(contexts: Set<Context>, reportType: String, resolvedData: List<PropertyData>): ReportProblems {
        val targetPeriod = if (reportType == "10-Q") 90 else 365
        val suspiciousContexts = contexts
            .filter {
                it.period == null || (!it.period!!.isInstant && abs(it.period!!.duration - targetPeriod) > 21)
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
