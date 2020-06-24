package fi.avp.edgar

import org.litote.kmongo.KMongo
import org.litote.kmongo.aggregate

open class ReportAnalyser {

    fun getCompaniesNotReportingNetIncomeLoss(): List<String?> {
        return Database.reports.getReportsOfType(reportType = "netIncome").filter {
            it.reports.none { it.series.any { it._id == "NetIncomeLoss" } }
        }.map { it.ticker }
    }

    fun getCompaniesWithEmptyNetIncomeReport(): List<String?> {
        return Database.reports.getReportsOfType(reportType = "netIncome").filter {
            it.reports.all { it.series.isEmpty() }
        }.map { it.ticker }
    }
}

fun main(args: Array<String>) {
//    ReportAnalyser().getCompaniesNotReportingNetIncomeLoss().forEach { println(it) }
    ReportAnalyser().getCompaniesWithEmptyNetIncomeReport().forEach { println(it) }
}

