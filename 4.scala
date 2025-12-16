package be.openthebox.modules.adhoc

import be.openthebox.modules.organizations.CalcOrganizationAggregates
import be.openthebox.modules.annualaccounts.CalcVatsWithAnnualAccountSummary
import be.openthebox.dto._ // OrganisationBO, VatWithAnnualAccountSummary, AnnualAccountSummary, etc.
import be.openthebox.Utils

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

object ExportNacebel56WithFinancials2019_2023ToCsv extends App {

  implicit val spark: SparkSession =
    SparkSession.builder()
      .appName("Export NACEBEL 56 with Financials 2019-2023")
      .getOrCreate()

  import spark.implicits._

  val yearFrom = 2019
  val yearTo = 2023
  val nacePrefix = "56"

  // -----------------------------
  // Load datasets
  // -----------------------------
  val organizationDS: Dataset[OrganisationBO] =
    CalcOrganizationAggregates.loadResultsFromParquet

  val annualAccountDS: Dataset[VatWithAnnualAccountSummary] =
    CalcVatsWithAnnualAccountSummary.loadResultsFromParquet

  // -----------------------------
  // Filter organizations by NACEBEL 56
  // -----------------------------
  val nace56Orgs = organizationDS
    .filter { org =>
      org.activities.exists { act =>
        // CUSTOMIZE: if you want to restrict to NACEBEL only, keep the classification check
        // act.classification == "NACEBEL" &&
        Option(act.code).exists(_.startsWith(nacePrefix))
      }
    }
    .select(
      $"vat",
      $"nameNl".as("nameNl"),
      $"nameFr".as("nameFr"),
      $"legalStatus",
      $"foundingDate",
      $"activities"
    )
    .cache()

  // -----------------------------
  // Prepare annual accounts (2019-2023) flattened to (vat, year, metrics...)
  // -----------------------------
  val financials2019_2023 = annualAccountDS
    .select($"vat", explode($"annualAccountSummaries").as("aas"))
    .select(
      $"vat",
      $"aas.periodEndDate".as("periodEndDate"),
      year($"aas.periodEndDate").as("year"),

      // P&L metrics (as per data model reference)
      $"aas.profitLoss.turnoverO".as("turnover"),
      $"aas.profitLoss.ebitdaO".as("ebitda"),
      $"aas.profitLoss.operatingProfitO".as("operatingProfit"),
      $"aas.profitLoss.profitAfterTaxO".as("profitAfterTax"),

      // Other common metrics
      $"aas.numberOfEmployees".as("employees"),
      $"aas.schemaType".as("schemaType"),
      $"aas.currency".as("currency")
    )
    .filter($"year".between(yearFrom, yearTo))

  // -----------------------------
  // Join & export
  // -----------------------------
  val outDf =
    nace56Orgs
      .join(financials2019_2023, Seq("vat"), "left_outer")
      .select(
        $"vat",
        $"nameNl",
        $"nameFr",
        $"legalStatus",
        $"foundingDate",

        $"year",
        $"periodEndDate",
        $"schemaType",
        $"currency",

        $"turnover",
        $"ebitda",
        $"operatingProfit",
        $"profitAfterTax",
        $"employees"
      )
      .orderBy($"vat", $"year")

  // Uses project utility which typically writes semicolon-separated CSV with header
  Utils.saveAsCsvFile(outDf, s"NACEBEL56_Financials_${yearFrom}_${yearTo}.csv")

  nace56Orgs.unpersist()
}
