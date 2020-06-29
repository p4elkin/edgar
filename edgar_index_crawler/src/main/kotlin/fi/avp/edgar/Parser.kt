package fi.avp.edgar

import fi.avp.edgar.data.PropertyDescriptor
import fi.avp.edgar.data.ReportMetadata
import fi.avp.edgar.data.attrNames
import fi.avp.util.mapAsync
import kotlinx.coroutines.*
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.codecs.pojo.annotations.BsonProperty
import org.litote.kmongo.eq
import org.litote.kmongo.setTo
import org.litote.kmongo.updateOne
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.time.LocalDate
import java.util.concurrent.Executors

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
        val parseReports = parseReports("AAPL")
        parseReports
    }
}

suspend fun parseReports(ticker: String): List<Pair<ReportReference, List<PropertyData>>?> {
        val reportReferences = Database.getReportReferences(ticker)
        val reportData = getCompanyReports(ticker)

        return reportReferences.mapAsync { reportReference ->
            val data = reportData["${reportReference.reference}.xml"]
            data?.let {
                val report = Report(
                    it.byteInputStream(StandardCharsets.UTF_8),
                    reportReference.dateFiled?.atStartOfDay()!!,
                    reportReference.formType!!)

                val resolvedData = attrNames.map { report.resolveProperty(it) }
                val contexts = getNonAmbiguousContexts(resolvedData)
                val extractedProperties = disambiguateProperties(contexts, resolvedData)
                val allProps = contexts.flatMap {
                    report.extractAllPropertiesForContext(it.id)
                }.map {
                    PropertyData(PropertyDescriptor(
                        variants = listOf(it.propertyId),
                        id = it.propertyId,
                        category = "misc"
                    ), listOf(it))
                }
                reportReference to allProps
            }
        }.awaitAll()
}

val coroutineDispatcher: CoroutineDispatcher = Executors.newFixedThreadPool(16).asCoroutineDispatcher()
suspend fun resolveMetrics(companyName: String) = coroutineScope {
    withContext(coroutineDispatcher) {
        streamCompanyReports(companyName).mapAsync { (metadata, file) ->
            extractMetrics(file, metadata)
        }.awaitAll()
    }.toMap()
}

private fun extractMetrics(file: Path, metadata: ReportMetadata): Pair<ReportMetadata, List<PropertyData>> {
    return file.toFile().inputStream().use {
        val report = Report(it, metadata.date, metadata.reportType)
        val resolvedData = attrNames.map { report.resolveProperty(it) }
        val contexts = getNonAmbiguousContexts(resolvedData)
        val extractedProperties = disambiguateProperties(contexts, resolvedData)
        val allProps = contexts.flatMap {
            report.extractAllPropertiesForContext(it.id)
        }.map {
            PropertyData(PropertyDescriptor(
                variants = listOf(it.propertyId),
                id = it.propertyId,
                category = "misc"
            ), listOf(it))
        }

        metadata to extractedProperties.plus(allProps)
    }
}

fun disambiguateProperties(nonAmbiguousContexts: Set<Context>, data: List<PropertyData>): List<PropertyData> {
    return data.map {
        val contexts = it.extractedValues.map { it.context }.toSet()
        var contextSet = contexts.filter { nonAmbiguousContexts.contains(it) }
        if (contextSet.isEmpty()) {
            contextSet = contexts.filter { it.segment == null }
        }

        val filteredValues = it.extractedValues.filter {
            it.context in contextSet
        }

        PropertyData(
            it.descriptor,
            filteredValues
        )
    }
}

fun getNonAmbiguousContexts(props: List<PropertyData>): Set<Context> {
    return props.filter {
        it.extractedValues.groupBy { it.propertyId }.all { it.value.size == 1 }
    }.flatMap {
        it.extractedValues.map { it.context }
    }.toSet()
}




