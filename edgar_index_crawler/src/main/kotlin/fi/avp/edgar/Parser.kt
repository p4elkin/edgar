package fi.avp.edgar

import fi.avp.edgar.data.PropertyDescriptor
import fi.avp.edgar.data.attrNames
import fi.avp.util.mapAsync
import kotlinx.coroutines.*
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.codecs.pojo.annotations.BsonProperty
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.concurrent.Executors
import kotlin.math.abs
import kotlin.system.measureTimeMillis

data class Metric(
    @BsonProperty(value = "type")
    val type: String,
    val category: String,
    val value: String,
    val unit: ValueUnit?,
    val contextId: String,
    val sourcePropertyName: String)

data class ReportRecord(
    val cik: String,
    val name: String,
    @BsonId val _id: String,
    val type: String,
    val date: LocalDate,
    val dataUrl: String,
    val reportFileName: String,
    val relatedContexts: List<Context> = emptyList(),
    val metrics: List<Metric>?,
    val extracts: Map<String, String>
)

val coroutineDispatcher: CoroutineDispatcher = Executors.newFixedThreadPool(8).asCoroutineDispatcher()
fun main() {
    runBlocking {
//        getCompanyNames().filter { it == "apple_inc" }.forEach {
//            resolveMetrics(it).forEach { (metadata, properties) ->
//                println("saving ${metadata.companyRef.name} for ${metadata.date}")
//                val metrics = properties.flatMap { propertyData ->
//                    val descriptor = propertyData.descriptor
//                    propertyData.extractedValues.map {
//                        Metric(
//                            descriptor.id,
//                            descriptor.category,
//                            it.value,
//                            it.unit,
//                            it.context.id,
//                            it.propertyId)
//                    }
//                }
//
//                Database.reports.updateOne(ReportRecord::_id eq metadata.getReportId(), ReportRecord::metrics setTo metrics)
//            }
//        }
        Database.getSP500Tickers().map { if (it == "GOOGL") "GOOG" else it } .forEach {
            val doneIn = measureTimeMillis {
                withContext(coroutineDispatcher) {
                    val parsedReports = parseReports(it)
                    Database.update(parsedReports)

                    parsedReports.forEach {
                        if (it.second.data.isEmpty()) {
                            println("empty report for: ${it.first.reference} (${it.first.dataUrl})")
                        }

                        if (it.second.problems.suspiciousContexts.isNotEmpty() || it.second.problems.missingProperties.isNotEmpty()) {
                            println("${it.first.reference} (${it.first.dataUrl}) contains issues: ${it.second.problems}")
                        }
                    }
                }
            }
            println("parsed $it in $doneIn ms")
        }
    }
}

val noProblems = ReportProblems(emptySet(), emptySet())
suspend fun parseReports(ticker: String): List<Pair<ReportReference, ReportDataExtractionResult>> {
        val reportReferences = Database.getReportReferences(ticker)
        val reportData = getCompanyReports(ticker)

        return reportReferences.mapAsync { reportReference ->
            println("resolving ${reportReference.dataUrl}")
            val data = reportData["${reportReference.reference}.xml"]
            if (data == null) {
                println("missing report data for: ${reportReference.reference}")
            }
            data?.let {
                try {
                    reportReference to parseProps(it.byteInputStream(StandardCharsets.UTF_8), reportReference.dateFiled?.atStartOfDay()!!, reportReference.formType!!)
                } catch (e: Exception) {
                    println("failed to parse report from ${reportReference.reference}.xml (${reportReference.dataUrl})")
                    null
                }

            } ?: reportReference to ReportDataExtractionResult(emptyList(), noProblems)
        }.awaitAll()
}

data class ReportDataExtractionResult(
    val data: List<PropertyData>,
    val problems: ReportProblems
)

data class ReportProblems(
    val suspiciousContexts: Set<String>,
    val missingProperties: Set<PropertyDescriptor>
) {
    override fun toString(): String {
        val ctx = suspiciousContexts.map { "$it" }.joinToString()
        val prop = missingProperties.map { it.id }.joinToString()
        return "$ctx and $prop"
    }
}


fun parseProps(data: InputStream, date: LocalDateTime, reportType: String): ReportDataExtractionResult {
    val report = Report(data, date, reportType)

    val resolvedData = attrNames.map { report.resolveProperty(it) }
    val allContexts = resolvedData.flatMap { it.contexts }.map { it.id to it }.toMap()

    val contexts = disambiguateProperties(getNonAmbiguousContexts(resolvedData), allContexts.values.toSet(), resolvedData).flatMap {
        it.extractedValues.map { report.contextById(it.context)!! }
    }.toSet()

    val problems = sanitiseExtractedData(contexts, reportType, resolvedData)

    val data = contexts.filter { it.id !in problems.suspiciousContexts }.flatMap {
        report.extractAllPropertiesForContext(it.id)
    }.map {
        PropertyData(
            PropertyDescriptor(variants = listOf(it.propertyId), id = it.propertyId, category = "misc"),
            extractedValues = listOf(it),
            contexts = listOf(allContexts[it.context]!!),
            valueUnits = it.unit?.let { listOf(report.unitById(it)!!) })
    }

    return ReportDataExtractionResult(data, problems)
}

fun sanitiseExtractedData(contexts: Set<Context>, reportType: String, resolvedData: List<PropertyData>): ReportProblems {
    val targetPeriod = if (reportType == "10-Q") 90 else 365
    val suspiciousContexts = contexts
        .filter {
            it.period == null || (!it.period.isInstant && abs(it.period.duration - targetPeriod) > 3)
        }
        .map { it.id }
        .toSet()

    val missingProperties = attrNames.filter { propertyDescriptor ->
        resolvedData
            .filter { propertyData -> propertyData.extractedValues.any { it.context !in suspiciousContexts } }
            .none { it.descriptor == propertyDescriptor }
    }.toSet()

    return ReportProblems(suspiciousContexts = suspiciousContexts, missingProperties = missingProperties)
}

fun disambiguateProperties(nonAmbiguousContexts: Set<String>, allContexts: Set<Context>, data: List<PropertyData>): List<PropertyData> {
    return data.map {
        val contexts = it.extractedValues
            .map { it.context }
            .map { contextId -> allContexts.find { it.id == contextId }!! }
            .toSet()

        var contextSet = contexts
            .filter {nonAmbiguousContexts.contains(it.id) }

        if (contextSet.isEmpty()) {
            contextSet = contexts.filter { it.segment == null }
        }

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

fun getNonAmbiguousContexts(props: List<PropertyData>): Set<String> {
    return props.filter {
        it.extractedValues.groupBy { it.propertyId }.all { it.value.size == 1 }
    }.flatMap {
        it.extractedValues.map { it.context }
    }.toSet()
}




