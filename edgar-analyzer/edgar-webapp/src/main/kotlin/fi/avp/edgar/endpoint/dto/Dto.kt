package fi.avp.edgar.endpoint.dto

import fi.avp.edgar.Filing
import fi.avp.edgar.valueInMillions
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.bson.conversions.Bson
import org.litote.kmongo.eq
import org.litote.kmongo.gt
import org.litote.kmongo.gte
import org.litote.kmongo.lte
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId

fun Filing.toDto(): FilingDTO {
    val reportLink = files?.visualReport ?: ""
    val interactiveData = fileName?.replace(".txt", "-index.htm")?.let {
        if (!it.contains("www.sec.gov/Archives/")) {
            "https://www.sec.gov/Archives/$it"
        } else it
    } ?: ""

    return FilingDTO(
        _id!!.toString(),
        ticker ?: "unknown",
        companyName!!,
        reportLink,
        interactiveData = interactiveData,
        date = dateFiled!!,
        type = formType!!,
        epsYY = eps?.relativeYearToYearChange() ?: Double.NaN,
        eps = eps?.value ?: Double.NaN,
        cashIncome = valueInMillions(cashIncome),

        revenueYY = revenue?.relativeYearToYearChange() ?: Double.NaN,
        revenue = valueInMillions(revenue),

        netIncomeYY = netIncome?.relativeYearToYearChange() ?: Double.NaN,
        netIncome = valueInMillions(netIncome),

        liabilitiesYY = liabilities?.relativeYearToYearChange() ?: Double.NaN,
        liabilities = valueInMillions(liabilities),
        latestAnnualRevenue = valueInMillions(latestRevenue),
        fiscalYear = fiscalYear
    )
}

@Serializable
data class FilingDTO(
    val id: String,
    val ticker: String,
    val name: String,
    val reportLink: String,
    val interactiveData: String,
    val fiscalYear: Long?,
    @Contextual
    val date: LocalDate,
    val type: String,
    val epsYY: Double,
    val eps: Double,
    val revenueYY: Double,
    val revenue: Double,
    val netIncomeYY: Double,
    val netIncome: Double,
    val liabilitiesYY: Double,
    val liabilities: Double,
    val latestAnnualRevenue: Double,
    val cashIncome: Double?
)


data class Filter(
    val annualOnly: Boolean?,
    val withMissingRevenue: Boolean?,
    val startDate: Long?,
    val endDate: Long?,
    val company: String?,
    val revenueThreshold: Long = 1000_000_000) {

    fun toBson(): Bson {
        val companyFilter = company?.let {
            com.mongodb.BasicDBObject("\$text", com.mongodb.BasicDBObject("\$search", it))
        }

        val dateFilter = org.litote.kmongo.and(
            fi.avp.edgar.Filing::dateFiled gte (startDate?.let { localDateFromMillis(it) } ?: LocalDate.of(
                2011,
                1,
                1
            )),
            fi.avp.edgar.Filing::dateFiled lte (endDate?.let { localDateFromMillis(it) } ?: LocalDate.now()))


        var bsonFilter = org.litote.kmongo.and(
            dateFilter,
            companyFilter
        )

        if (annualOnly == true) {
            bsonFilter = org.litote.kmongo.and(bsonFilter, fi.avp.edgar.Filing::formType eq "10-K")
        }

        if (withMissingRevenue == true) {
            bsonFilter = org.litote.kmongo.and(bsonFilter, fi.avp.edgar.Filing::revenue eq null)
        } else {
            bsonFilter = org.litote.kmongo.and(
                bsonFilter,
                fi.avp.edgar.Filing::latestRevenue gt revenueThreshold.toDouble()
            )
        }

        return bsonFilter
    }

    private fun localDateFromMillis(millis: Long) =
        Instant.ofEpochMilli(millis).atZone(ZoneId.systemDefault()).toLocalDate()
}
