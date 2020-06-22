package fi.avp.edgar.data

import fi.avp.edgar.EDGAR_DATA
import java.time.LocalDate
import java.time.LocalDateTime

data class CompanyRef(val cik: String,
                      val name: String)

data class ReportMetadata(val companyRef: CompanyRef,
                          val year: String,
                          val quarter: String,
                          val reportType: String,
                          val date: LocalDateTime,
                          val reportPath: String) {

    fun getReportId(): String {
        return getReportDataURL().substringAfterLast("/")
    }

    fun getReportDataURL(): String {
        val reportId = reportPath.let {
            val id = it.substringAfterLast("/")
            id.substring(0, id.length - 4)
        }

        val reportBaseUrl = "$EDGAR_DATA${companyRef.cik}/${reportId.replace("-", "")}"
        return reportBaseUrl
    }

}

data class ValueUnit(val id: String, val measure: String?, val divide: Pair<String, String>?)
data class Period(val startDate: LocalDate, val endDate: LocalDate, val isInstant: Boolean = false) {
    val duration: Long
        get() = java.time.Duration.between(startDate.atStartOfDay(), endDate.atStartOfDay()).toDays()
}

data class Context(val id: String,  val period: Period?, val segment: String? = null)


data class PropertyDescriptor(val variants: List<String>, val base: String, val id: String = "", val category: String = "")
val attrNames: List<PropertyDescriptor> = listOf(
    PropertyDescriptor(listOf(
        "Assets"
    ), "Assets", "assets", "balance"),
    PropertyDescriptor(listOf(
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
        "Equity", "stockHolderEquity", "balance"),

    PropertyDescriptor(listOf(
//        "ElectricalDistributionRevenue",
        "SegmentReportingInformationRevenue",
        "RevenuesNetOfInterestExpense",
        "ElectricalTransmissionAndDistributionRevenue",
        "RevenueMineralSales",
        "Revenues",
        "OilAndGasRevenue",
        "SalesRevenueGoodsNet",
        "SalesRevenueNet",
        "TotalRevenuesAndOtherIncome",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "RevenueFromContractWithCustomerExcludingAssessedTax"
    ), "Revenue", "revenue", "operations"),
    PropertyDescriptor(listOf(
        "ProfitLoss",
        "NetIncomeLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic"),
        "Income", "netIncome", "operations"),
    PropertyDescriptor(listOf(
//        "IncreaseDecreaseInOtherOperatingCapitalNet",
        "OperatingIncomeLoss",
        "GrossProfit"
//        "ComprehensiveIncomeLoss",
//        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
//        "IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest",
//        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"
    ),
        "Income", "operatingIncome", "operations"),

    PropertyDescriptor(listOf(
        "NetCashProvidedByUsedInInvestingActivities",
        "NetCashProvidedByUsedInInvestingActivitiesContinuingOperations"),
        "NetCash", "investingCashFlow", "cashFlow"),
    PropertyDescriptor(listOf(
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"),
        "NetCash", "operatingCashFlow", "cashFlow"),
    PropertyDescriptor(listOf(
        "NetCashProvidedByUsedInFinancingActivitiesContinuingOperations",
        "NetCashProvidedByUsedInFinancingActivities"),
        "NetCash", "financingCashFlow", "cashFlow")

//        "",
//us-gaap:NetCashProvidedByUsedInFinancingActivities, (???)
)
