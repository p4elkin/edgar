package fi.avp.edgar

import kotlinx.serialization.Serializable
import kotlin.math.ceil

@Serializable
data class Metric(
    val value: Double? = 0.0,
    val unit: String?,
    val context: String,
    val involvedProperties: Set<String>,
    val formula: String,
    val notes: String? = null,
    val yearToYearChange: Double? = 0.0) {

    fun calculateYearToYear(previous: Metric?): Metric? {
        return previous?.let {
            copy(yearToYearChange = value!! - it.value!!)
        } ?: this
    }

    fun relativeYearToYearChange(): Double {
        return ceil(value!! / (value - yearToYearChange!!) * 10000) / 10000
    }
}

fun valueInMillions(metric: Metric?): Double {
    return valueInMillions(metric?.value)
}

fun valueInMillions(value: Double?): Double {
    return value?.let {
        val millions = (it / 1000_000.0)
        if (millions < 1) {
            millions
        } else {
            ceil(millions * 100) / 100
        }
    } ?: Double.NaN
}

open class NumberExtractor(val variants: Set<String>) {

    fun get(filing: Filing): Double? {
        return filing.extractedData?.find {
            it.propertyId in variants
        }?.numericValue()
    }
}

object InvestingCashFlow: MetricExtractor(
    variants = setOf(
        "NetCashProvidedByUsedInInvestingActivities",
        "NetCashProvidedByUsedInInvestingActivitiesContinuingOperations"),
    primaryVariants = setOf("NetCashProvidedByUsedInInvestingActivities"),
    compoundVariants = emptySet()
)

object FinancingCashFlow: MetricExtractor(
    variants = setOf(
        "NetCashProvidedByUsedInFinancingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"),
    primaryVariants = setOf("NetCashProvidedByUsedInFinancingActivities"),
    compoundVariants = emptySet()
)

object Liabilities: MetricExtractor(
    variants = setOf(
        "Liabilities"),
    primaryVariants = setOf("Liabilities"),
    compoundVariants = emptySet()) {

    fun subtractEquityFromAssets(filing: Filing): Metric? {
        val assets = Assets.get(filing)
        val equity = Equity.get(filing)

        return if (assets?.value != null && equity?.value != null) {
            Metric(
                value = assets.value - equity.value,
                unit = assets.unit,
                context = assets.context,
                formula = "[${assets.formula}] - [${equity.formula}]",
                involvedProperties = assets.involvedProperties.plus(equity.involvedProperties)
            )
        } else {
            null
        }
    }

    override fun get(filing: Filing): Metric? {
        return super.get(filing) ?: subtractEquityFromAssets(filing)
    }
}



object OperatingCashFlow: MetricExtractor(
    variants = setOf(
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"),
    primaryVariants = setOf("NetCashProvidedByUsedInOperatingActivities"),
    compoundVariants = emptySet())

object FiscalYearExtractor: NumberExtractor(setOf("dei:DocumentFiscalYearFocus"))
object SharesOutstandingExtractor: NumberExtractor(setOf("dei:EntityCommonStockSharesOutstanding", "CommonStockSharesAuthorized"))

open class MetricExtractor(val variants: Set<String>, val primaryVariants: Set<String>, val compoundVariants: Set<String>) {

    open fun get(filing: Filing): Metric? {
        val possibleValue = filing.extractedData?.find {
            it.propertyId in variants
        }

        return possibleValue?.let {
            try {
                Metric(
                    value = it.numericValue(),
                    unit = it.unit,
                    context = it.context,
                    formula = it.propertyId,
                    involvedProperties = setOf(it.propertyId),
                    notes = null
                )
            } catch (e: Exception) {
                Metric(
                    value = null,
                    unit = it.unit,
                    context = it.context,
                    formula = it.propertyId,
                    involvedProperties = setOf(it.propertyId),
                    notes = e.message
                )
            }
        } ?: extractCompoundMetric(filing)
    }


    private fun extractCompoundMetric(report: Filing): Metric? {
        val compoundMetrics = report.extractedData?.filter {
            it.propertyId in compoundVariants
        } ?: emptyList()

        if (compoundMetrics.isEmpty()) {
            return null
        }

        val compoundValue = compoundMetrics.map { it.numericValue() }.sum()
        val allContexts = compoundMetrics.map { it.context }.toSet()
        val units = compoundMetrics.map { it.unit }.filterNotNull().toSet()

        return Metric(
            value = compoundValue,
            context = allContexts.joinToString(separator = "/"),
            unit = units.joinToString(separator = "/"),
            involvedProperties = compoundMetrics.map { it.propertyId }.toSet(),
            formula = compoundMetrics.map { it.propertyId }.joinToString(separator = "+"),
            notes = null
        )
    }
}

object NetIncome: MetricExtractor(
    variants = setOf(
        "ProfitLoss",
        "NetIncomeLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic"),
    primaryVariants = setOf("ProfitLoss"),
    compoundVariants = emptySet()
)

object Eps: MetricExtractor(
    variants = setOf(
        "EarningsPerShareDiluted",
        "EarningsPerShareBasicAndDiluted",
        "EarningsPerShareBasic",
        "IncomeLossFromContinuingOperationsPerDilutedShare",
        "IncomeLossFromContinuingOperationsPerBasicShare",
        "fast_BasicDilutedEarningsPerShareNetIncome"),
    primaryVariants = setOf("ProfitLoss"),
    compoundVariants = emptySet()
)

object Equity: MetricExtractor(
    variants = setOf("StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
    primaryVariants = setOf("StockholdersEquity"),
    compoundVariants = emptySet()
)

object Assets: MetricExtractor(
    variants = setOf("Assets"),
    primaryVariants = setOf("Assets"),
    compoundVariants = emptySet()
)

object Revenue: MetricExtractor(
    variants = setOf(
        "RevenuesExcludingInterestAndDividends",
        "HealthCareOrganizationRevenue",
        "Revenues",
        "SalesRevenueNet",
        "SegmentReportingInformationRevenue",
        "RevenuesNetOfInterestExpense",
        "ElectricalTransmissionAndDistributionRevenue",
        "RevenueMineralSales",
        "OilAndGasRevenue",
        "SalesRevenueGoodsNet",
        "SalesRevenueServicesNet",
        "TotalRevenuesAndOtherIncome",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "RevenueFromContractWithCustomerExcludingAssessedTax"),
    compoundVariants = setOf(
        "ElectricUtilityRevenue",
        "ElectricalDistributionRevenue"),
    primaryVariants = setOf(
        "Revenues",
        "SalesRevenueNet")
)


fun main() {
}
