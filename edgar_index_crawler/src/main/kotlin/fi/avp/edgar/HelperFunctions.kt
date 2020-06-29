package fi.avp.edgar

import fi.avp.util.*
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.litote.kmongo.save

fun ensureCikPaddingsInTickerMappings() {
    val updated = Database.tickers.find().map {
        it.copy(cik = it.cik.padStart(10, '0'), ticker = it.ticker.toUpperCase())
    }.distinctBy { it.cik }

    Database.tickers.drop()
    Database.tickers.insertMany(updated)
}

fun ensureCikPaddingsInReport() {
    val tickers = Database.tickers.find().toList()
    val reportRefs = Database.reportIndex.find().toList()
    val updated = reportRefs.map {
        it.copy(ticker = tickers
            .find { tickerMapping -> it.cik?.padStart(10, '0') == tickerMapping.cik}?.ticker)
    }

    Database.reportIndex.drop()
    Database.reportIndex.insertMany(updated)
}

fun resolveFileNames() {
    val reports = Database.reportIndex.find().toList()
    reports.groupBy { it.ticker }.filter { it.key != null }.forEach { companyReports ->
        println("Downloading ${companyReports.key}")
        repeat(3) {
            try {
                runBlocking {
                    val downloadedStuff = companyReports.value.filter { it.reportFiles == null }
                        .mapAsync {
                            it.copy(reportFiles = fetchRelevantFileNames(it))
                        }.awaitAll()

                    downloadedStuff.forEach { Database.reportIndex.save(it) }
                    if (downloadedStuff.isNotEmpty()) {
                        delay(1000)
                    }
                }
                return@forEach
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }
}

fun updateDate() {
    val reports = Database.reportIndex.find().toList()
    Database.reportIndex.drop()
    Database.reportIndex.insertMany(reports)
}

fun updateDataUrl() {
    val reports = Database.reportIndex.find().toList()
    val backup = Database.database.getCollection("report-index-bak", ReportReference::class.java)

    backup.drop()
    backup.insertMany(reports)

    Database.reportIndex.drop()
    Database.reportIndex.insertMany(reports)
}

fun main() {
    downloadXBRL()
}

