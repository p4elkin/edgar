package fi.avp.edgar

import fi.avp.edgar.data.*
import fi.avp.util.attr
import fi.avp.util.find
import fi.avp.util.list
import org.w3c.dom.Document
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import java.io.InputStream
import java.lang.RuntimeException
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeParseException
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPath
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathFactory


data class PropertyData(val descriptor: PropertyDescriptor, val extractedValues: List<ExtractedValue>)

data class ExtractedValue (
    val propertyId: String,
    val value: String,
    val decimals: String,
    // power of 10
    val scale: String,
    val context: Context,
    val unit: ValueUnit?)

data class ValueUnit(
    val id: String,
    val measure: String?,
    val divide: Pair<String, String>?)

data class Period(val startDate: LocalDate, val endDate: LocalDate, val isInstant: Boolean = false) {
    val duration: Long
        get() = Duration.between(startDate.atStartOfDay(), endDate.atStartOfDay()).toDays()
}

data class Context(val id: String,  val period: Period?, val segment: String? = null)

open class Report(content: InputStream, val date: LocalDateTime, private val reportType: String) {

    private val contextCache = hashMapOf<String, Context?>()
    private val unitCache = hashMapOf<String, ValueUnit?>()

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
            throw RuntimeException(e)
        }
    }

    private val isInline = reportDoc.documentElement.tagName == "html"

    private fun actualiseMetrics(nodes: List<ExtractedValue>): List<ExtractedValue> {
        val closestToReportDate =  nodes.groupBy {
                val endDate= it.context.period?.endDate?.atStartOfDay() ?: date;
                Duration.between(endDate, date)
            }.minBy {
                it.key
            }?.value ?: emptyList()

        if (closestToReportDate.size == 1) {
            return closestToReportDate
        }

        return closestToReportDate.groupBy {
            val duration = it.context.period?.duration ?: 0
            if (reportType == "10-Q") 90 - duration else 365 - duration
        }.maxBy {
            it.key
        }?.value ?: emptyList()
    }

    fun getPropertyName(node: Node): String {
       return if (isInline) node.attr("name")!! else node.nodeName
    }

    fun extractAllPropertiesForContext(contextId: String): List<ExtractedValue> {
        return resolveNodesReferencingContext(contextId).list().map {
            ExtractedValue(
                propertyId = it.nodeName.substringAfter("us-gaap:", getPropertyName(it)),
                value = it.textContent,
                decimals = it.attr("decimals") ?: "",
                scale = it.attr("scale") ?: "",
                context = contextById(contextId)!!,
                unit = it.attr("unitRef")?.let {unitById(it)}
            )
        }
    }

    fun resolveProperty(propertyDescriptor: PropertyDescriptor): PropertyData {
        // Fetch all the variants
        val resolvedVariants = propertyDescriptor.variants
            .flatMap { variantId ->
                resolveAttrNodes(variantId).list().map {
                    // we always assume that ctx is there
                    val ctx = contextById(it.attr("contextRef")?.trim()!!)

                    //
                    val unit = it.attr("unitRef")?.let {
                        unitById(it)
                    }

                    if (ctx == null) {
                        throw RuntimeException()
                    }

                    ExtractedValue(variantId, it.textContent, context = ctx, unit = unit,
                        scale = it.attr("scale") ?: "", decimals = it.attr("decimals") ?: "")
                }
            }

        return PropertyData(propertyDescriptor, actualiseMetrics(resolvedVariants))
    }

    private fun resolveNodesReferencingContext(contextId: String): NodeList {
        val selector = "//*[@contextRef='${contextId}']"
        return xpath.compile(selector).evaluate(reportDoc, XPathConstants.NODESET) as NodeList
    }

    private fun resolveAttrNodes(attrId: String): NodeList {
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
}
