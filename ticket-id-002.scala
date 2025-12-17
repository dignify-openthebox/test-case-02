package be.openthebox.modules.adhoc

import be.openthebox.dto._
import be.openthebox.modules.calc._
import be.openthebox.utils.Utils
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date
import java.time.LocalDate

object ExportNacebel56Companies2019_2023ToCsv extends App {

  implicit val spark: SparkSession =
    SparkSession.builder()
      .appName("Export NACEBEL 56 Companies with Financials 2019-2023")
      .getOrCreate()

  import spark.implicits._

  // -----------------------------
  // CUSTOMIZE: choose year window
  // -----------------------------
  val fromYear = 2019
  val toYear = 2023

  // -----------------------------
  // Load datasets
  // -----------------------------
  val organizationDS: Dataset[OrganisationBO] =
    CalcOrganizationAggregates.loadResultsFromParquet

  val vatWithAnnualAccountSummaryDS: Dataset[VatWithAnnualAccountSummary] =
    CalcVatsWithAnnualAccountSummary.loadResultsFromParquet

  // -----------------------------
  // Helpers
  // -----------------------------
  def yearOf(d: Date): Int = d.toLocalDate.getYear

  def pickCurrentAddressFormatted(org: OrganisationBO): Option[String] = {
    def formatAddress(ah: AddressHist): Option[String] = {
      val adO = ah.addressNlO.orElse(ah.addressFrO)
      adO.map { ad =>
        val parts = List(
          Option(ad.street).filter(_.nonEmpty),
          Option(ad.number).filter(_.nonEmpty),
          Option(ad.zipCode).filter(_.nonEmpty),
          Option(ad.city).filter(_.nonEmpty),
          ad.provinceO.filter(_.nonEmpty),
          Option(ad.countryCode).filter(_.nonEmpty)
        ).flatten
        parts.mkString(", ")
      }
    }

    val current = org.addressHists.find(_.endDateO.isEmpty)
      .orElse(org.addressHists.sortBy(_.startDateO.map(_.getTime).getOrElse(Long.MinValue)).lastOption)

    current.flatMap(formatAddress)
  }

  def yearFounded(org: OrganisationBO): Option[Int] = {
    // CUSTOMIZE: depending on your DTO, founding date field name might differ.
    // Common patterns: org.startDateO, org.foundingDateO, org.dateFoundationO, etc.
    // Keep it defensive:
    val candidates: Seq[Option[Date]] = Seq(
      org.startDateO,          // seen in several DTOs
      org.foundingDateO        // if available in your build
    ).distinct

    candidates.flatten.headOption.map(d => d.toLocalDate.getYear)
  }

  // -----------------------------
  // 1) Filter companies by NACEBEL 56
  // -----------------------------
  val nace56Companies = organizationDS
    .filter { org =>
      org.activities.exists { act =>
        act.classification == "NACEBEL" && Option(act.code).exists(_.startsWith("56"))
      }
    }
    .map { org =>
      CompanyBaseRow(
        vat = org.vat,
        name = Option(org.nameNl).filter(_.nonEmpty)
          .orElse(Option(org.nameFr).filter(_.nonEmpty))
          .getOrElse(""),
        address = pickCurrentAddressFormatted(org).getOrElse(""),
        yearFounded = yearFounded(org),
        // CUSTOMIZE: if your OrganisationBO uses another field name, update here.
        financialHealthIndicatorOrgLevel = org.financialHealthIndicatorO
      )
    }
    .cache()

  // -----------------------------
  // 2) Flatten annual accounts to (vat, year, metrics) for 2019-2023
  // -----------------------------
  val annuals2019_2023 = vatWithAnnualAccountSummaryDS
    .flatMap { vaa =>
      vaa.annualAccountSummaries.toSeq
        .filter(aas => {
          val y = yearOf(aas.periodEndDate)
          y >= fromYear && y <= toYear
        })
        .map { aas =>
          val y = yearOf(aas.periodEndDate)

          AnnualMetricsRow(
            vat = vaa.vat,
            year = y,
            financialHealthIndicator = aas.financialAnalysisO.flatMap(_.financialHealthIndicator),
            equity = aas.balanceSheet.equityO.flatMap(_.currentO),
            grossMargin = aas.profitLoss.grossMarginO,
            profit = aas.balanceSheet.gainLossPeriodO.flatMap(_.currentO),
            ebitda = aas.financialAnalysisO.flatMap(_.ebitda),
            employees = aas.numberOfEmployees
          )
        }
    }
    .toDF()
    // If multiple filings end in same year, keep the latest periodEndDate is better,
    // but we only have "year" here. If needed, extend AnnualMetricsRow with periodEndDate.
    .dropDuplicates("vat", "year")
    .as[AnnualMetricsRow]

  // -----------------------------
  // 3) Join company base + annual metrics and write CSV
  // -----------------------------
  val out = nace56Companies
    .joinWith(annuals2019_2023, nace56Companies("vat") === annuals2019_2023("vat"), "left_outer")
    .map { case (c, a) =>
      Nace56ExportRow(
        vat = c.vat,
        name = c.name,
        address = c.address,
        yearFounded = c.yearFounded,
        year = Option(a).map(_.year),
        // Prefer annual-account indicator when present; fallback to org-level
        financialHealthIndicator = Option(a).flatMap(_.financialHealthIndicator).orElse(c.financialHealthIndicatorOrgLevel),
        equity = Option(a).flatMap(_.equity),
        grossMargin = Option(a).flatMap(_.grossMargin),
        profit = Option(a).flatMap(_.profit),
        ebitda = Option(a).flatMap(_.ebitda),
        employees = Option(a).flatMap(_.employees)
      )
    }
    .toDF()

  Utils.saveAsCsvFile(
    out,
    s"NACEBEL56_companies_financials_${fromYear}_${toYear}.csv"
  )

  nace56Companies.unpersist()
}

case class CompanyBaseRow(
  vat: String,
  name: String,
  address: String,
  yearFounded: Option[Int],
  financialHealthIndicatorOrgLevel: Option[Double]
)

case class AnnualMetricsRow(
  vat: String,
  year: Int,
  financialHealthIndicator: Option[Double],
  equity: Option[Double],
  grossMargin: Option[Double],
  profit: Option[Double],
  ebitda: Option[Double],
  employees: Option[Int]
)

case class Nace56ExportRow(
  vat: String,
  name: String,
  address: String,
  yearFounded: Option[Int],
  year: Option[Int],
  financialHealthIndicator: Option[Double],
  equity: Option[Double],
  grossMargin: Option[Double],
  profit: Option[Double],
  ebitda: Option[Double],
  employees: Option[Int]
)
