package fi.avp.edgar

import fi.avp.edgar.util.Locations
import fi.avp.edgar.util.Locations.reports
import fi.avp.edgar.util.mapAsync
import fi.avp.edgar.util.nullOnFailure
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.litote.kmongo.eq
import java.nio.file.Files
import java.nio.file.Paths

suspend fun downloadXBRL() {
    Database.getSP500Companies().forEach {
        if (!Files.exists(Paths.get("${Locations.reports}/${it.primaryTicker}.zip"))) {
            val primaryTicker = it.primaryTicker
            println("Downloading $primaryTicker")
            downloadReports(
                primaryTicker,
                it.tickers.flatMap { Database.getFilingsByTicker(it) })
        }
    }
}

suspend fun downloadReports(ticker: String, filings: List<Filing>) = coroutineScope {
    println("Downloading ${ticker}")
    filings
        .sortedByDescending { it.dateFiled }
        .filter { it.files != null }
        .distinctBy { it.files!!.xbrlReport }
        .mapAsync { filing ->
            nullOnFailure(errorMessage = {"Failed to download XBRL data for ${filing.dataUrl} due to ${it.message}"}) {
                filing.withBasicFilingData()
            }
        }
        .awaitAll()
        .filterNotNull()
}

fun main() {
    runBlocking {
        Database.filings.find("{formType: '10-K'}").toList().chunked(5).forEach {
            it.mapAsync {
                it.withFiles()
            }.awaitAll().forEach {
                Database.filings.updateOne(Filing::_id eq it._id, it)
            }
        }
        downloadXBRL()
    }
}
