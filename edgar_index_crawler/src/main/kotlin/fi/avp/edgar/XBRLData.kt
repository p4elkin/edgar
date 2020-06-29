package fi.avp.edgar

import fi.avp.util.getReportData
import java.util.*
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream

private fun ZipInputStream.getEntries(): Map<String, String> {
    val seq: Sequence<ZipEntry?> = generateSequence { nextEntry }
    return seq.takeWhile { it != null }
        .map {
            val sc = Scanner(this);
            var shouldContinue = true
            val sb = StringBuilder()
            while (sc.hasNextLine()) {
                sb.appendln(sc.nextLine())
            }
            it!!.name to sb.toString()
        }
        .toMap()
}

fun getCompanyReports(ticker: String): Map<String, String> {
    return getReportData(ticker)?.getEntries() ?: emptyMap()
}
