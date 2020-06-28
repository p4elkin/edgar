package fi.avp.edgar

import org.litote.kmongo.KMongo
import java.time.LocalDate

object Database {
    val client = KMongo.createClient() //get com.mongodb.MongoClient new instance
    val database = client.getDatabase("sec-report") //normal java driver usage
    val reports = database.getCollection("reports", ReportRecord::class.java)
    val reportIndex = database.getCollection("report-index", ReportReference::class.java)
    val xbrl = database.getCollection("xbrl", XBRL::class.java)
    val ticker = database.getCollection("ticker", TickerMapping::class.java)
}


data class XBRL(val dataUrl: String, val xbrl: String?)

data class ReportReference(
    val _id: String,
    val cik: String?,
    val dateFiled: LocalDate?,
    val fileName: String?,
    val companyName: String?,
    val formType: String?,
    val reportFile: String?,
    val ticker: String?,
    var dataUrl: String?,
    val reportFiles: ReportFiles?
) {
    init {
        val reportId = fileName.let {
            val id = it?.substringAfterLast("/")
            id?.substring(0, id.length - 4)
        }

        dataUrl = "$EDGAR_DATA${cik}/${reportId?.replace("-", "")}"
    }
}

data class TickerMapping(val _id: String, val ticker: String, val cik: String)
