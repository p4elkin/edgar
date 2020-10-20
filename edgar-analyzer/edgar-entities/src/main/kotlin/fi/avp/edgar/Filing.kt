package fi.avp.edgar

import com.github.michaelbull.retry.policy.constantDelay
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import fi.avp.edgar.util.*
import kotlinx.coroutines.awaitAll
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import kotlinx.serialization.KSerializer
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.descriptors.*
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.Serializer

import org.bson.types.ObjectId
import java.io.*
import java.lang.Exception
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDate
import java.time.Duration
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream

import kotlin.math.pow

enum class OperationStatus {
    DONE,
    FAILED,
    PENDING,
    MISSING
}

@Serializable
data class PropertyDescriptor(
    val variants: List<String>,
    val id: String = "",
    val category: String = ""
)

@Serializable
data class ValueUnit(
    val id: String,
    val measure: String?,
    val divide: Pair<String, String>?)

@Serializable
data class Period(
    @Contextual val startDate: LocalDate,
    @Contextual val endDate: LocalDate, val isInstant: Boolean = false) {
    val duration: Long
        get() = Duration.between(startDate.atStartOfDay(), endDate.atStartOfDay()).toDays()
}

@Serializer(forClass = LocalDate::class)
object DateSerializer: KSerializer<LocalDate> {
    private val df = DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a")

    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("WithCustomDefault", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, obj: LocalDate) {
        encoder.encodeString(df.format(obj))
    }

    override fun deserialize(decoder: Decoder): LocalDate {
        return LocalDate.from(df.parse(decoder.decodeString()))
    }
}

@Serializable
data class Context(val id: String, val period: Period?, val segment: String? = null)

@Serializable
data class ReportProblems(val suspiciousContexts: Set<String>, val missingProperties: Set<PropertyDescriptor>) {
    override fun toString(): String {
        val ctx = suspiciousContexts.map { it }.joinToString()
        val prop = missingProperties.map { it.id }.joinToString()
        return "$ctx and $prop"
    }
}

@Serializable
data class PropertyData(
    val descriptor: PropertyDescriptor,
    val extractedValues: List<ExtractedValue>,
    val contexts: Set<Context>,
    val valueUnits: Set<ValueUnit>?
)

@Serializable
data class ExtractedValue (
    val propertyId: String,
    val value: String,
    val decimals: String,
    val isNegative: Boolean = false,
    // power of 10
    val scale: String,
    val context: String,
    val unit: String?,
    // value difference since last report
    val delta: Double? = 0.0) {

    fun numericValue(): Double {
        return try {
            try {
                parseValue(value, decimals.toDouble()) * (if (isNegative) -1 else 1)
            } catch(e: Exception) {
                value.replace(",", "").toDouble()
            }

        } catch(e: Exception) {
            if (value.isBlank() || value == "—" || value == "-") 0.0 else Double.NaN
        }
    }

    private fun parseValue(value: String, decimals: Double): Double {
        val withoutSeparator: Double = value.replace(",", "").replace(".", "").toDouble()
        val ratio = 10.0.pow(-decimals)

        return if (withoutSeparator % ratio != 0.0) {
            withoutSeparator * ratio
        } else {
            withoutSeparator
        }
    }

}


@Serializable
data class Filing(
    @Contextual
    var _id: ObjectId? = null,
    val cik: Long?,

    @Contextual
    val dateFiled: LocalDate?,

    val fileName: String? = null,
    val companyName: String?,
    val formType: String?,

    val revenue: Metric? = null,
    val netIncome: Metric? = null,
    val equity: Metric? = null,
    val investingCashFlow: Metric? = null,
    val operatingCashFlow: Metric? = null,
    val financingCashFlow: Metric? = null,
    val liabilities: Metric? = null,
    val sharesOutstanding: Long? = null,
    val eps: Metric? = null,
    val assets: Metric? = null,
    val cashIncome: Double? = null,

    val fiscalYear: Long? = null,
    val ticker: String? = null,
    var dataUrl: String?,
    val problems: ReportProblems? = null,
    val contexts: Set<Context>? = emptySet(),
    val units: Set<ValueUnit>? = emptySet(),
    val extractedData: List<ExtractedValue>? = emptyList(),
    var reference: String? = null, // last segment of data URL
    val files: ReportFiles? = null,

    @Contextual
    val closestYearReportId: ObjectId? = null,
    val latestRevenue: Double? = null,

    val fileResolutionStatus: OperationStatus = OperationStatus.PENDING,
    val dataExtractionStatus: OperationStatus? = OperationStatus.PENDING,
    val yearToYearUpdate: OperationStatus? = OperationStatus.PENDING) {

    init {
        fileName?.let {
            if (dataUrl == null) {
                dataUrl = if (fileName.endsWith(".txt")) {
                    val reportId = fileName.let {
                        val id = it.substringAfterLast("/")
                        id.substring(0, id.length - 4)
                    }

                    "$EDGAR_DATA${cik}/${reportId?.replace("-", "")}"
                } else {
                    "$EDGAR_DATA${cik}/${it.replace("-", "")}"
                }
            }

            reference = dataUrl?.substringAfterLast("/")
        }
    }



    @Serializable
    data class ReportFiles(
        val visualReport: String? = null,
        val xbrlReport: String? = null,
        val cashFlow: String? = null,
        val income: String? = null,
        val operations: String? = null,
        val balance: String? = null,
        val financialSummary: String? = null) {

        suspend fun xbrlData(url: String): String? {
            return xbrlReport?.let { getFileContents(it, url) }
        }

        public suspend fun getFileContents(name: String, at: String): String? {
            return ZipInputStream(getReportZip(at).toFile().inputStream().buffered()).use { zip ->
                generateSequence { zip.nextEntry }.find { it.name == name }?.let {
                    val sc = Scanner(zip);
                    val sb = StringBuilder()
                    while (sc.hasNextLine()) {
                        sb.appendln(sc.nextLine())
                    }
                    sb.toString()
                }
            }
        }

        suspend fun getReportZip(url: String): Path {
            suspend fun fetchFiles(): Map<String, String> = retry(limitAttempts(3) + constantDelay(5000)) {
                listOfNotNull(xbrlReport, income, cashFlow, balance, operations)
                    .mapAsync { it to asyncGetText("$url/$it") }
                    .awaitAll()
                    .toMap()
            }

            fun createZipEntry(reportFileName: String, data: String?, out: ZipOutputStream) {
                data?.let {
                    BufferedInputStream(it.byteInputStream(StandardCharsets.UTF_8)).use { origin ->
                        val zipEntry = ZipEntry(reportFileName)
                        out.putNextEntry(zipEntry)
                        origin.copyTo(out, 8 * 1024)
                    }
                }
            }

            val path = Locations.reports.resolve("$xbrlReport.zip")
            if (!Files.exists(path)) {
                println("$path missing, creating")
                val fileData = fetchFiles()
                // if file still doesn't exist
                if (!Files.exists(path)) {
                    val newZipFile = Files.createFile(path).toFile()
                    ZipOutputStream(newZipFile.outputStream().buffered()).use { out ->
                        fileData.forEach { (name, data) ->
                            createZipEntry(name, data, out)
                        }
                    }
                }
            }

            return path
        }

        suspend fun cashFlow(dataUrl: String): String? {
            return cashFlow?.let {
                getFileContents(it, dataUrl)
            }
        }

        suspend fun balance(dataUrl: String): String? {
            return balance?.let {
                getFileContents(it, dataUrl)
            }
        }

        suspend fun income(dataUrl: String): String? {
            return income?.let {
                getFileContents(it, dataUrl)
            }
        }

        suspend fun operations(dataUrl: String): String? {
            return operations?.let {
                getFileContents(it, dataUrl)
            }
        }
    }
}
