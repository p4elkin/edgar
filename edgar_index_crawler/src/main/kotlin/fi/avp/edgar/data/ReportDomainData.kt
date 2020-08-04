package fi.avp.edgar.data

import fi.avp.edgar.mining.EDGAR_DATA
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

data class PropertyDescriptor(
    val variants: List<String>,
    val id: String = "",
    val category: String = ""
)
val attrNames: List<PropertyDescriptor> = listOf(
    PropertyDescriptor(
        listOf(
            "Assets"
        ), "assets", "balance"
    ),
    PropertyDescriptor(
        listOf(
            "StockholdersEquity",
            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
        "stockHolderEquity", "balance"
    ),

    PropertyDescriptor(
        listOf(
    //        "ElectricalDistributionRevenue",
            "ElectricUtilityRevenue",
            "SegmentReportingInformationRevenue",
            "RevenuesNetOfInterestExpense",
            "ElectricalTransmissionAndDistributionRevenue",
            "RevenueMineralSales",
            "Revenues",
            "OilAndGasRevenue",
            "SalesRevenueGoodsNet",
            "SalesRevenueNet",
            "SalesRevenueServicesNet",
            "TotalRevenuesAndOtherIncome",
            "RevenueFromContractWithCustomerIncludingAssessedTax",
            "RevenueFromContractWithCustomerExcludingAssessedTax"
        ), "revenue", "operations"
    ),
    PropertyDescriptor(
        listOf(
            "ProfitLoss",
            "NetIncomeLoss",
            "NetIncomeLossAvailableToCommonStockholdersBasic"),
        "netIncome", "operations"
    ),
    PropertyDescriptor(
        listOf(
    //        "IncreaseDecreaseInOtherOperatingCapitalNet",
            "OperatingIncomeLoss",
            "GrossProfit",
            "UtilityRevenue"
    //        "ComprehensiveIncomeLoss",
    //        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
    //        "IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest",
    //        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"
        ),
        "operatingIncome", "operations"
    ),

    PropertyDescriptor(
        listOf(
            "NetCashProvidedByUsedInInvestingActivities",
            "NetCashProvidedByUsedInInvestingActivitiesContinuingOperations"),
        "investingCashFlow", "cashFlow"
    ),
    PropertyDescriptor(
        listOf(
            "NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"),
        "operatingCashFlow", "cashFlow"
    ),
    PropertyDescriptor(
        listOf(
            "NetCashProvidedByUsedInFinancingActivitiesContinuingOperations",
            "NetCashProvidedByUsedInFinancingActivities"),
        "financingCashFlow", "cashFlow"),
    PropertyDescriptor(listOf(
        "EarningsPerShareDiluted",
        "EarningsPerShareBasicAndDiluted",
        "IncomeLossFromContinuingOperationsPerDilutedShare",
        "IncomeLossFromContinuingOperationsPerBasicShare",
        "EarningsPerShareBasic",
        "fast_BasicDilutedEarningsPerShareNetIncome"
        //WeightedAverageNumberOfDilutedSharesOutstanding
        //WeightedAverageNumberBasicDilutedSharesOutstanding
// EarningsPerShareDiluted
    ), id = "eps", category = "eps")
)

//        "",
//us-gaap:NetCashProvidedByUsedInFinancingActivities, (???)
