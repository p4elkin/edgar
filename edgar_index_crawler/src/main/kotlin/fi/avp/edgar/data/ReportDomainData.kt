package fi.avp.edgar.data

import fi.avp.edgar.mining.EDGAR_DATA
import kotlinx.serialization.Serializable
import java.time.LocalDateTime

@Serializable
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
