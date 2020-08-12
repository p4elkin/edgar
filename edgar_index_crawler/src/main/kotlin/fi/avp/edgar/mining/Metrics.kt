package fi.avp.edgar.mining

import kotlinx.serialization.Serializable
import kotlin.math.ceil

@Serializable
data class Metric(
    val value: Double? = 0.0,
    val unit: String?,
    val context: String,
    val involvedProperties: Set<String>,
    val formula: String,
    val notes: String?,
    val yearToYearChange: Double? = 0.0) {

    fun calculateYearToYear(previous: Metric?): Metric? {
        return previous?.let {
            copy(yearToYearChange = value!! - it.value!!)
        }
    }

    fun relativeYearToYearChange(): Double {
        return ceil(value!! / (value - yearToYearChange!!) * 100) / 100
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

    fun get(report: Filing): Double? {
        return report.extractedData?.find {
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
    compoundVariants = emptySet())

object OperatingCashFlow: MetricExtractor(
    variants = setOf(
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"),
    primaryVariants = setOf("NetCashProvidedByUsedInOperatingActivities"),
    compoundVariants = emptySet())

object FiscalYearExtractor: NumberExtractor(setOf("dei:DocumentFiscalYearFocus"))

open class MetricExtractor(val variants: Set<String>, val primaryVariants: Set<String>, val compoundVariants: Set<String>) {

    fun get(report: Filing): Metric? {
        val possibleValue = report.extractedData?.find {
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
        } ?: extractCompoundMetric(report)
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

object Assets: MetricExtractor(
    variants = setOf("Assets"),
    primaryVariants = setOf("Assets"),
    compoundVariants = emptySet()
)

object Revenue: MetricExtractor(
    variants = setOf(
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
