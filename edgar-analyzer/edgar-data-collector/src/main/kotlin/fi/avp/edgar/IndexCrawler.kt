package fi.avp.edgar

import com.fasterxml.jackson.databind.JsonNode
import fi.avp.edgar.util.*
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import okhttp3.Request
import java.nio.file.Files
import java.nio.file.Paths
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

data class QuarterIndex(
    val year: String,
    val quarterId: String,
    val xbrlData: String)

data class ArchiveEntry(
    val url: String,
    val sizeKb: Int,
    val name: String,
    val lastModified: LocalDateTime
)

suspend fun getFilingsAfter(date: LocalDate): List<ArchiveEntry> {
    // TODO - we can crawl across quarters, also need to be able to
    // crawl across years
    return crawl(DAILY_INDEX).filter { it.lastModified.year >= date.year }
            .map { it.url }.flatMap {
                yearUrl ->
                    crawl(yearUrl).map {it.url}.flatMap {
                        crawl(it)
                                .filter { it.name.startsWith("crawler") }
                                .filter { it.lastModified.isAfter(date.atStartOfDay()) }
                    }

            }
}

suspend fun crawl(url: String): List<ArchiveEntry> {
    val data = asyncJson(
        Request.Builder()
            .url("$url/index.json")
            .build())

    return data["directory"]["item"].map {
        val href = it.text("href")
        val sizeKb = it.text("size")
            .replace(" KB", "")
            .toInt()
        val lastModified = LocalDateTime.parse(
            it.text("last-modified"),
            DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a"))

        ArchiveEntry(
            url = "$url${href}",
            sizeKb = sizeKb,
            lastModified = lastModified,
            name = it.text("name")
        )
    }
}

fun main() {
    val index = runBlocking {
        // fetch the root structure of the EDGAR database
        val yearlyIndices = crawl()["directory"]["item"]
            .filter { it.text("type") == "dir" }
            .mapAsync {
                val year = it.text("href").trim('/')
                val yearDataIndexUrl = "$EDGAR_INDEX_URL${year}"
                val getQuarterlyIndex = Request.Builder()
                    .url("${yearDataIndexUrl}/index.json")
                    .build()

                val quarterlyIndex = asyncJson(getQuarterlyIndex)["directory"]["item"]
                quarterlyIndex
                    .mapAsync {
                        val quarterId = it.text("href").trim('/')
                        QuarterIndex(
                            year = year,
                            quarterId = quarterId,
                            xbrlData = asyncGetText("${yearDataIndexUrl}/${quarterId}/crawler.idx")
                        )
                    }
                    .awaitAll()
            }
            .awaitAll()
            .flatten()

        yearlyIndices
    }

    val currentDir = Paths.get("").toAbsolutePath().parent.resolve("data/crawler-index")
    if (!Files.exists(currentDir)) {
        Files.createDirectory(currentDir)
    }

    index.groupBy { it.year }.forEach { (year, quarterlyIndices) ->
        val yearlyIndexDirectoryName = currentDir.resolve(year)
        try {
            if (Files.exists(yearlyIndexDirectoryName)) {
                return@forEach
            }

            Files.createDirectory(yearlyIndexDirectoryName)
            quarterlyIndices.forEach { quarterIndex ->
                val quarterlyDirectoryPath = yearlyIndexDirectoryName.resolve(quarterIndex.quarterId)
                val quarterlyDirectory = Files.createDirectory(quarterlyDirectoryPath);

                Files.createFile(quarterlyDirectory.resolve(REPORT_INDEX_FILE_NAME))
                    .toFile()
                    .writeText(quarterIndex.xbrlData)
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

}

suspend fun crawl(): JsonNode {
    return asyncJson(
        Request.Builder()
            .url("$EDGAR_INDEX_URL/index.json")
            .build()
    )
}

