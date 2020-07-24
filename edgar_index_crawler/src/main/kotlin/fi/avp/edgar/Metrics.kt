package fi.avp.edgar

import fi.avp.util.mapAsync
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.litote.kmongo.find
import org.litote.kmongo.getCollection
import org.litote.kmongo.updateOne
import java.util.concurrent.Executors

data class Metric(
    val value: Double?,
    val unit: String?,
    val context: String,
    val involvedProperties: Set<String>,
    val formula: String,
    val notes: String?)

open class NumberExtractor(val variants: Set<String>) {

    fun get(report: ReportReference): Double? {
        return report.extractedData?.find {
            it.propertyId in variants
        }?.numericValue()
    }
}

object InvestingCashFlow:MetricExtractor(
    variants = setOf(
        "NetCashProvidedByUsedInInvestingActivities",
        "NetCashProvidedByUsedInInvestingActivitiesContinuingOperations"),
    primaryVariants = setOf("NetCashProvidedByUsedInInvestingActivities"),
    compoundVariants = emptySet()
)

object FinancingCashFlow:MetricExtractor(
    variants = setOf(
        "NetCashProvidedByUsedInFinancingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"),
    primaryVariants = setOf("NetCashProvidedByUsedInFinancingActivities"),
    compoundVariants = emptySet()
)

object Liabilities:MetricExtractor(
    variants = setOf(
        "Liabilities"),
    primaryVariants = setOf("Liabilities"),
    compoundVariants = emptySet())

object OperatingCashFlow:MetricExtractor(
    variants = setOf(
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"),
    primaryVariants = setOf("NetCashProvidedByUsedInOperatingActivities"),
    compoundVariants = emptySet())

object FiscalYearExtractor:NumberExtractor(setOf("dei:DocumentFiscalYearFocus"))

open class MetricExtractor(val variants: Set<String>, val primaryVariants: Set<String>, val compoundVariants: Set<String>) {

    fun get(report: ReportReference): Metric? {
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
                    notes = null)
            } catch (e: Exception) {
                Metric(
                    value = null,
                    unit = it.unit,
                    context = it.context,
                    formula = it.propertyId,
                    involvedProperties = setOf(it.propertyId),
                    notes = e.message)
            }
        } ?: extractCompoundMetric(report)
    }


    private fun extractCompoundMetric(report: ReportReference): Metric? {
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

object NetIncome:MetricExtractor(
    variants = setOf(
        "ProfitLoss",
        "NetIncomeLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic"),
    primaryVariants = setOf("ProfitLoss"),
    compoundVariants = emptySet()
)

object Eps:MetricExtractor(
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

object Assets:MetricExtractor(
    variants = setOf("Assets"),
    primaryVariants = setOf("Assets"),
    compoundVariants = emptySet()
)

object Revenue:MetricExtractor(
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

fun extractMetrics(update: (ReportReference) -> ReportReference) {
    val reports = Database.database.getCollection<ReportReference>("sp500")
    val allReports = reports
        .find("{formType: '10-K'}")
        .batchSize(10000).toList()

    println("analyzing ${allReports.size} reports")
    runBlocking {
        withContext(Executors.newFixedThreadPool(16).asCoroutineDispatcher()) {
            allReports.chunked(100).mapAsync {
                it.forEach {
                    reports.updateOne(update(it))
                }
            }
        }
        println("done")
    }
}

fun main() {
}
