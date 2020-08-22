package fi.avp.edgar

import fi.avp.edgar.PropertyDescriptor
import kotlinx.serialization.Serializable

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
    //      "ElectricalDistributionRevenue",
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
            "OperatingIncomeLoss",
            "GrossProfit",
            "UtilityRevenue"
    //      "IncreaseDecreaseInOtherOperatingCapitalNet",
    //      "ComprehensiveIncomeLoss",
    //      "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
    //      "IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest",
    //      "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"
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
