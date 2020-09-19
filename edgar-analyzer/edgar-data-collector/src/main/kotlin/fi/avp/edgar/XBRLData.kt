package fi.avp.edgar

import fi.avp.edgar.util.getReportData
import fi.avp.edgar.util.getReportDataZip
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream

private fun ZipInputStream.getEntries(): Map<String, InputStream> {
    val seq: Sequence<ZipEntry?> = generateSequence { nextEntry }
    return seq.takeWhile { it != null }
        .map {
            val sc = Scanner(this);
            val sb = StringBuilder()
            while (sc.hasNextLine()) {
                sb.appendln(sc.nextLine())
            }
            it!!.name to sb.toString().byteInputStream(StandardCharsets.UTF_8)
        }
        .toMap()
}

fun getCompanyReports(ticker: String): Map<String, InputStream> {
    return try {
        getReportData(ticker) ?:
        getReportDataZip(ticker)?.getEntries() ?:
        emptyMap()
    } catch (e: Exception) {
        e.printStackTrace()
        return emptyMap()
    }
}
