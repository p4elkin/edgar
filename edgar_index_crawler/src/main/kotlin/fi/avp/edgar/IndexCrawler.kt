package fi.avp.edgar

import com.fasterxml.jackson.databind.JsonNode
import fi.avp.util.asyncGetText
import fi.avp.util.asyncJson
import fi.avp.util.mapAsync
import fi.avp.util.text
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import okhttp3.Request
import java.nio.file.Files
import java.nio.file.Paths

const val EDGAR_DATA = "https://www.sec.gov/Archives/edgar/data/"
const val EDGAR_INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index/"
const val REPORT_INDEX_FILE_NAME = "report_index.csv"

data class QuarterIndex(val year: String, val quarterId: String, val xbrlData: String)

fun main() {
    val index = runBlocking {
        // fetch the root structure of the EDGAR database
        val yearlyIndices = crawl()["directory"]["item"]
            .filter { it.text("type") == "dir" }
            .mapAsync {
                val year = it.text("href").trim('/')
                val yearDataIndexUrl = "${EDGAR_INDEX_URL}${year}"
                val getQuarterlyIndex = Request.Builder()
                    .url("${yearDataIndexUrl}/index.json")
                    .build()

                val quarterlyIndex = asyncJson(getQuarterlyIndex)["directory"]["item"]
                quarterlyIndex
                    .mapAsync {
                        val quarterId = it.text("href").trim('/')
                        QuarterIndex(year = year, quarterId = quarterId, xbrlData = asyncGetText("${yearDataIndexUrl}/${quarterId}/xbrl.idx"))
                    }
                    .awaitAll()
            }
            .awaitAll()
            .flatten()

        yearlyIndices
    }

    val currentDir = Paths.get("").toAbsolutePath().parent.resolve("data/report_indices")
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
            .url("${EDGAR_INDEX_URL}/index.json")
            .build()
    )
}

