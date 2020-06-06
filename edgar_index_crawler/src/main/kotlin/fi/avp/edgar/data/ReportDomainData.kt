package fi.avp.edgar.data

import java.time.LocalDateTime
import java.time.temporal.Temporal

data class CompanyRef(val cik: String,
                      val name: String)

data class ReportRecord(val companyRef: CompanyRef,
                        val year: String,
                        val quarter: String,
                        val reportType: String,
                        val date: LocalDateTime,
                        val reportPath: String)
