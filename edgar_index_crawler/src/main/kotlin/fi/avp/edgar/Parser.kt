package fi.avp.edgar

import fi.avp.util.mapAsync
import kotlinx.coroutines.*
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.codecs.pojo.annotations.BsonProperty
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.concurrent.Executors
import kotlin.system.measureTimeMillis


data class ReportRecord(
    val cik: String,
    val name: String,
    @BsonId val _id: String,
    val type: String,
    val date: LocalDate,
    val dataUrl: String,
    val reportFileName: String,
    val relatedContexts: List<Context> = emptyList(),
    val extracts: Map<String, String>
)

private val taskDispatcher: CoroutineDispatcher = Executors.newFixedThreadPool(16).asCoroutineDispatcher()

fun main() {
    runBlocking {
        parseLargeCapReports()
    }
}

private suspend fun parseLargeCapReports() {
    Database.getSP500Tickers()
        .map { if (it == "GOOGL") "GOOG" else it }
//        .filter { it == "AEP"}
        .forEach {
            val doneIn = measureTimeMillis {
                val reportReferences = Database.getReportReferences(it)
                withContext(taskDispatcher) {
                    val reportData = getCompanyReports(it)
                    val parsedReports = parseReports(reportReferences, reportData)

                    parsedReports.forEach {
                        if (it.second.data.isNotEmpty()) {
                            println("saving ${reportReferences.size} reports")
                            Database.storeExtractedData(it.first, it.second)
                        }
                    }
                }
            }
            println("parsed $it in $doneIn ms")
        }
}

suspend fun parseReports(reportReferences: List<ReportReference>, reportData: Map<String, InputStream>): List<Pair<ReportReference, ReportDataExtractionResult>> {
    return reportReferences
        .mapAsync { reportReference ->
            println("resolving ${reportReference.dataUrl}")
            val data = reportData["${reportReference.reference}.xml"]
            if (data == null) {
                println("missing report data for: ${reportReference.reference}")
            }
            data?.let {
                try {
                    val inputStream = it
                    val date = reportReference.dateFiled?.atStartOfDay()!!
                    val isInline = reportReference.reportFiles!!.xbrlReport!!.endsWith(".htm")
                    reportReference to parseProps(inputStream, date, reportReference.formType!!, isInline)
                } catch (e: Exception) {
                    println("failed to parse report from ${reportReference.reference}.xml (${reportReference.dataUrl})")
                    e.printStackTrace()
                    null
                }

            } ?: reportReference to ReportDataExtractionResult(emptyList(), noProblems)
        }.awaitAll()
}

fun parseProps(data: InputStream, date: LocalDateTime, reportType: String, isInline: Boolean): ReportDataExtractionResult {
    return Report(data, date, reportType, isInline).extractData()
}





