package fi.avp.edgar

import org.litote.kmongo.KMongo
import org.litote.kmongo.aggregate

open class ReportAnalyser {

    val client = KMongo.createClient() //get com.mongodb.MongoClient new instance
    val database = client.getDatabase("sec-report") //normal java driver usage
    val collection = database.getCollection("annual", ReportRecord::class.java)

    fun getCompaniesNotReportingNetIncomeLoss(): List<String?> {
        return collection.getReportsOfType(reportType = "netIncome").filter {
            it.reports.none { it.series.any { it._id == "NetIncomeLoss" } }
        }.map { it.ticker }
    }

    fun getCompaniesWithEmptyNetIncomeReport(): List<String?> {
        return collection.getReportsOfType(reportType = "netIncome").filter {
            it.reports.all { it.series.isEmpty() }
        }.map { it.ticker }
    }
}

fun main(args: Array<String>) {
//    ReportAnalyser().getCompaniesNotReportingNetIncomeLoss().forEach { println(it) }
    ReportAnalyser().getCompaniesWithEmptyNetIncomeReport().forEach { println(it) }
}

