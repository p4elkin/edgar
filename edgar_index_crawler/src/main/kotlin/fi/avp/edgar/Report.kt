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
import java.time.format.DateTimeParseException
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPath
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathFactory


data class ResolvedPropertyData(
    val propertyDescriptor: PropertyDescriptor,
    val resolvedNodes: List<PropertyRawData>,
    val alternatives: Set<String>)

data class PropertyRawData(
    val attrId: String,
    val value: String,
    val context: Context,
    val unit: ValueUnit?)

open class Report(content: InputStream, private val isInline: Boolean, val metadata: ReportMetadata) {

    private val contextCache = hashMapOf<String, Context?>()
    private val unitCache = hashMapOf<String, ValueUnit?>()

    private val xpath: XPath by lazy {
        XPathFactory.newInstance().newXPath()
    }

    private val reportDoc: Document by lazy {
        val builderFactory = DocumentBuilderFactory.newInstance()
        builderFactory.isNamespaceAware = true

        val builder = builderFactory.newDocumentBuilder()
        builder.parse(content)
    }

    private fun actualiseMetrics(nodes: List<PropertyRawData>): List<PropertyRawData> {
        val closestToReportDate =  nodes.groupBy {
                val endDate= it.context.period?.endDate?.atStartOfDay() ?: metadata.date;
                Duration.between(endDate, metadata.date)
            }.minBy {
                it.key
            }?.value ?: emptyList()

        if (closestToReportDate.size == 1) {
            return closestToReportDate
        }

        return closestToReportDate.groupBy {
            val duration = it.context.period?.duration ?: 0
            if (metadata.reportType == "10-Q") 90 - duration else 360 - duration
        }.maxBy {
            it.key
        }?.value ?: emptyList()
    }


    fun resolveProperty(propertyDescriptor: PropertyDescriptor): ResolvedPropertyData {
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

                    PropertyRawData(variantId, it.textContent, ctx!!, unit)
                }
            }


        // if none found - resolve some hints
        val alternatives = if (resolvedVariants.isEmpty()) {
            val similarNodes = nodeSet("//*[contains(local-name(), '${propertyDescriptor.base}')]")
            List(similarNodes.length) { similarNodes.item(it).nodeName!! }.toSet()
        } else emptySet()

        return ResolvedPropertyData(propertyDescriptor, actualiseMetrics(resolvedVariants), alternatives)
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
