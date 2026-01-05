package be.openthebox.modules.adhoc

import be.openthebox.model._
import be.openthebox.util.Utils
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

/**
 * Export companies with NACEBEL 56 (Food & beverage service activities), excluding establishments.
 *
 * Required fields for 2019–2023:
 * - Name
 * - Address
 * - Year founded
 * - Financial health indicator
 * - Equity
 * - Gross margin
 * - Profit
 * - EBITDA
 * - Employees
 *
 * Output:
 * - one row per (vat, year)
 *
 * CUSTOMIZE:
 * - output path / filename
 * - address formatting if you need a strict format matching OrganizationsDataCustom7_1.csv
 */
object ExportOrganizationsNacebel56NoEstablishmentsToCsv extends App {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Export Organizations NACEBEL 56 (no establishments) 2019-2023")
    .getOrCreate()

  import spark.implicits._

  // ----------------------------
  // Load datasets (project-standard loaders)
  // ----------------------------
  val organizationDS: Dataset[Organization] =
    CalcOrganizationAggregates.loadResultsFromParquet

  val vatWithAnnualAccountSummaryDS: Dataset[VatWithAnnualAccountSummary] =
    CalcVatsWithAnnualAccountSummary.loadResultsFromParquet

  // ----------------------------
  // Helpers
  // ----------------------------
  def formattedAddressFromHist(addressHist: AddressHist): Option[String] = {
    val adO: Option[be.openthebox.model.Address] = addressHist.addressNlO.orElse(addressHist.addressFrO)
    adO.map { ad =>
      val elements = List(
        ad.street,
        ad.number,
        ad.zipCode,
        ad.city,
        ad.provinceO.getOrElse(""),
        ad.countryCode
      ).map(_.trim).filter(_.nonEmpty)

      elements.mkString(", ")
    }
  }

  def currentOrLastAddress(org: Organization): Option[String] = {
    val current = org.addressHists
      .filter(_.endDateO.isEmpty)
      .sortBy(_.startDateO.map(_.getTime).getOrElse(Long.MinValue))
      .lastOption
      .flatMap(formattedAddressFromHist)

    current.orElse(
      org.addressHists
        .sortBy(_.startDateO.map(_.getTime).getOrElse(Long.MinValue))
        .lastOption
        .flatMap(formattedAddressFromHist)
    )
  }

  def accountingYearFromPeriodEnd(periodEnd: Date): Int = {
    // java.sql.Date#getYear is deprecated and returns year-1900; avoid it.
    val cal = java.util.Calendar.getInstance()
    cal.setTime(periodEnd)
    cal.get(java.util.Calendar.YEAR)
  }

  // ----------------------------
  // 1) Filter orgs by NACEBEL 56
  // ----------------------------
  val org56DS = organizationDS
    .filter { org =>
      org.activities.exists(a =>
        a.classification == "NACEBEL" && a.code != null && a.code.startsWith("56")
      )
    }
    .select(
      $"vat",
      $"nameNl",
      $"nameFr",
      $"foundingDateO",
      $"financialHealthIndicatorO", // org-level indicator (if present in your model)
      $"addressHists"
    )
    .cache()

  // ----------------------------
  // 2) Explode annual accounts 2019-2023
  // ----------------------------
  val fin2019_2023 = vatWithAnnualAccountSummaryDS
    .select($"vat", explode($"annualAccountSummaries").as("aas"))
    .select(
      $"vat",
      $"aas.periodEndDate".as("periodEndDate"),
      $"aas.balanceSheet.equityO.currentO".as("equity"),
      $"aas.profitLoss.grossMarginO".as("grossMargin"),
      // Profit: depending on schema, this may be "profitAfterTax" or "profitBeforeTax".
      // The KB snippet used balanceSheet.gainLossPeriodO.currentO; keep both and pick one.
      $"aas.profitLoss.profitAfterTaxO".as("profitAfterTax"),
      $"aas.profitLoss.profitBeforeTaxO".as("profitBeforeTax"),
      $"aas.balanceSheet.gainLossPeriodO.currentO".as("gainLossPeriod"),
      $"aas.profitLoss.ebitdaO".as("ebitda"),
      $"aas.numberOfEmployees".as("employees"),
      $"aas.financialAnalysisO.financialHealthIndicator".as("financialHealthIndicatorAA")
    )
    .withColumn("year", year($"periodEndDate"))
    .filter($"year".between(2019, 2023))

  // Decide which "profit" to output (priority order)
  val fin2019_2023_norm = fin2019_2023
    .withColumn(
      "profit",
      coalesce($"profitAfterTax", $"profitBeforeTax", $"gainLossPeriod")
    )
    .drop("profitAfterTax", "profitBeforeTax", "gainLossPeriod")

  // ----------------------------
  // 3) Join org base with financials and shape output
  // ----------------------------
  val resultDF = org56DS
    .join(fin2019_2023_norm, Seq("vat"), "left_outer")
    .map { row =>
      val vat = row.getAs[String]("vat")
      val nameNl = Option(row.getAs[String]("nameNl")).getOrElse("")
      val nameFr = Option(row.getAs[String]("nameFr")).getOrElse("")
      val foundingDateO = Option(row.getAs[Date]("foundingDateO"))
      val orgHealthO = Option(row.getAs[java.lang.Double]("financialHealthIndicatorO")).map(_.doubleValue())

      val addressHists = row.getAs[Seq[AddressHist]]("addressHists")
      val org = Organization(
        vat = vat,
        nameNl = nameNl,
        nameFr = nameFr,
        // the rest is not needed; we only need address history to format address
        // CUSTOMIZE: if Organization constructor differs in your codebase, replace this map() approach
        addressHists = addressHists.toArray
      )

      val address = currentOrLastAddress(org).getOrElse("")

      val year = Option(row.getAs[Int]("year"))
      val equity = Option(row.getAs[java.lang.Double]("equity")).map(_.doubleValue())
      val grossMargin = Option(row.getAs[java.lang.Double]("grossMargin")).map(_.doubleValue())
      val profit = Option(row.getAs[java.lang.Double]("profit")).map(_.doubleValue())
      val ebitda = Option(row.getAs[java.lang.Double]("ebitda")).map(_.doubleValue())
      val employees = Option(row.getAs[java.lang.Integer]("employees")).map(_.intValue())
      val aaHealthO = Option(row.getAs[java.lang.Double]("financialHealthIndicatorAA")).map(_.doubleValue())

      OutputRow(
        vat = vat,
        name = Option(nameNl).filter(_.nonEmpty).getOrElse(nameFr),
        address = address,
        yearFounded = foundingDateO.map(_.toString).getOrElse(""),
        accountingYear = year.map(_.toString).getOrElse(""),
        financialHealthIndicator = aaHealthO.orElse(orgHealthO).map(_.toString).getOrElse(""),
        equity = equity.map(_.toString).getOrElse(""),
        grossMargin = grossMargin.map(_.toString).getOrElse(""),
        profit = profit.map(_.toString).getOrElse(""),
        ebitda = ebitda.map(_.toString).getOrElse(""),
        employees = employees.map(_.toString).getOrElse("")
      )
    }
    .toDF()

  // Keep only rows that actually have a year (if you want to exclude companies without AA in 2019-2023)
  val finalDF = resultDF
    .filter($"accountingYear" =!= "")

  // ----------------------------
  // 4) Write CSV
  // ----------------------------
  // CUSTOMIZE: filename/path to match your "OrganizationsDataCustom7_1.csv" convention
  Utils.saveAsCsvFile(
    finalDF.orderBy($"vat", $"accountingYear"),
    "OrganizationsDataCustom7_1.csv"
  )

  org56DS.unpersist()
}

case class OutputRow(
  vat: String,
  name: String,
  address: String,
  yearFounded: String,
  accountingYear: String,
  financialHealthIndicator: String,
  equity: String,
  grossMargin: String,
  profit: String,
  ebitda: String,
  employees: String
)
