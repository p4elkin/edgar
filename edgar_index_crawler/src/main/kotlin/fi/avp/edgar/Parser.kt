package fi.avp.edgar

import com.mongodb.client.MongoCollection
import fi.avp.edgar.data.*
import fi.avp.util.mapAsync
import kotlinx.coroutines.*
import org.bson.codecs.pojo.annotations.BsonId
import org.litote.kmongo.*
import java.util.concurrent.Executors

fun main() {
    val client = KMongo.createClient() //get com.mongodb.MongoClient new instance
    val database = client.getDatabase("sec-report") //normal java driver usage
    val collection = database.getCollection("reports", ReportRecord::class.java)

    runBlocking {
        getCompanyNames().filter { it == "cbre_group_inc" }.forEach {
            resolveMetrics(it).forEach { (metadata, properties) ->
                println("saving ${metadata.companyRef.name} for ${metadata.date}")
                val metrics = properties.flatMap { propertyData ->
                    val descriptor = propertyData.propertyDescriptor
                    propertyData.resolvedNodes.map {
                        Metric(
                            descriptor.id,
                            descriptor.category,
                            it.value,
                            it.unit!!,
                            it.context.id,
                            it.attrId)
                    }
                }

                collection.updateOne(
                    ReportRecord::_id eq metadata.getReportId(),
                    ReportRecord::metrics setTo metrics)
            }
        }
    }
}

private fun __amendFinancingOperationRelatedMetrics(collection: MongoCollection<ReportRecord>) {
    getCompanyNames().filter { it == "apple_inc" }.forEach {
        val escapedName = it.replace("\\", "\\\\")
        collection.find("{name: '$escapedName'}").forEach { report ->
            if (report.metrics == null) {
                println("${report.name} ${report._id} ${report.dataUrl}")
            }
            val updatedMetrics = report.metrics?.map {
                if (it.sourcePropertyName == "NetCashProvidedByUsedInFinancingActivitiesContinuingOperations" ||
                    it.sourcePropertyName == "NetCashProvidedByUsedInFinancingActivities") {
                    it.copy(category = "cashFlow", type = "financingCashFlow")
                } else {
                    it
                }
            }

            collection.updateOne(report.copy(metrics = updatedMetrics))
        }
    }
}

val coroutineDispatcher: CoroutineDispatcher = Executors.newFixedThreadPool(16).asCoroutineDispatcher()
suspend fun resolveMetrics(companyName: String) = coroutineScope {
    withContext(coroutineDispatcher) {
        streamCompanyReports(companyName).mapAsync { (metadata, file) ->
            val path = file.toAbsolutePath().toString()
            println(path)
            val content = file.toFile().inputStream()
            content.use {
                val report = Report(it, path.endsWith(".htm"), metadata)
                val resolvedData = attrNames.map { report.resolveProperty(it) }
                metadata to disambiguateProperties(resolvedData)
            }
        }.awaitAll()
    }.toMap()
}

fun disambiguateProperties(resolvedData: List<ResolvedPropertyData>): List<ResolvedPropertyData> {
    val nonAmbiguousContexts = getNonAmbiguousContexts(resolvedData)
    return resolvedData.map {
        val contexts = it.resolvedNodes.map { it.context }.toSet()
        var contextSet = contexts.filter { nonAmbiguousContexts.contains(it) }
        if (contextSet.isEmpty()) {
            contextSet = contexts.filter { it.segment == null }
        }

        val filteredValues = it.resolvedNodes.filter {
            it.context in contextSet
        }

        ResolvedPropertyData(
            it.propertyDescriptor,
            filteredValues,
            it.alternatives
        )
    }
}

private fun getNonAmbiguousContexts(props: List<ResolvedPropertyData>): Set<Context> {
    return props.filter {
        it.resolvedNodes.groupBy { it.attrId }.all { it.value.size == 1 }
    }.flatMap {
        it.resolvedNodes.map { it.context }
    }.toSet()
}




