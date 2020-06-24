package fi.avp.edgar

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer
import org.bson.codecs.pojo.annotations.BsonProperty
import org.litote.kmongo.KMongo
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Database {
    val client = KMongo.createClient() //get com.mongodb.MongoClient new instance
    val database = client.getDatabase("sec-report") //normal java driver usage
    val reports = database.getCollection("reports", ReportRecord::class.java)
    val reportIndex = database.getCollection("report-index", ReportReference::class.java)
    val ticker = database.getCollection("ticker", TickerMapping::class.java)
}

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
