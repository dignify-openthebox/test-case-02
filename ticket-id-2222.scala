package be.openthebox.modules.adhoc

import be.openthebox.model
import be.openthebox.model.AddressHist
import be.openthebox.modules.{CalcOrganizationAggregates, Module}
import be.openthebox.util.{InputArgs, Utils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

/**
 * Export for companies with NACEBEL code 56 (company-level, excluding establishments),
 * enriched with annual accounts for years 2019–2023.
 *
 * Output: one row per (vat, year)
 */
object ExportOrganizationsNacebel56_2019_2023_ToCsv extends Module {

  override def execute(inputArgs: InputArgs)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession.sparkContext.setJobGroup(this.getClass.getName, this.getClass.getName)
    sparkSession.sparkContext.setJobDescription("Export NACEBEL 56 organizations with financials 2019-2023 to CSV")

    val startYear = 2019
    val endYear = 2023
    val nacePrefix = "56"

    // --- Helpers ------------------------------------------------------------

    def createFormattedAddress(addressHist: AddressHist): Option[String] = {
      val address: Option[model.Address] = addressHist.addressNlO.orElse(addressHist.addressFrO)
      address.map { ad =>
        val elements = List(
          Some(ad.number),
          Some(ad.street),
          Some(ad.zipCode),
          Some(ad.city),
          ad.provinceO,
          Some(ad.countryCode)
        )
        elements.flatten.map(_.toString).mkString(",")
      }
    }

    def bestName(org: model.OrganizationAggregate): String =
      Option(org.nameNl).filter(_.nonEmpty)
        .orElse(Option(org.nameFr).filter(_.nonEmpty))
        .getOrElse("")

    def foundedYear(org: model.OrganizationAggregate): Option[Int] = {
      // CUSTOMIZE: depending on your actual org aggregate fields, this may be `startDateO`, `foundingDateO`, ...
      // We try common patterns safely via reflection-free approach: keep as None if not available.
      // If your model has `startDateO: Option[java.sql.Date]`, replace below accordingly.
      None
    }

    // --- Load organizations -------------------------------------------------

    val orgDS = CalcOrganizationAggregates.loadResultsFromParquet

    val nace56OrgsDS = orgDS.filter { org =>
      // Filter by NACEBEL activities; codes may be 56 / 56.10 / ...
      org.activities.exists { act =>
        val codeOk = Option(act.code).exists(_.startsWith(nacePrefix))
        val classOk = Option(act.classification).forall(_.toUpperCase == "NACEBEL") // keep permissive if missing
        codeOk && classOk
      }
    }

    // Company-level only: we do NOT join establishments dataset.
    val orgBaseDF = nace56OrgsDS
      .map { org =>
        val lastAddress = org.addressHists.lastOption.flatMap(createFormattedAddress)

        (
          org.vat,
          bestName(org),
          lastAddress,
          foundedYear(org),                    // CUSTOMIZE (see above)
          Option(org.financialHealthIndicator) // CUSTOMIZE: if your aggregate already has it at org level
        )
      }
      .toDF("vat", "name", "address", "yearFounded", "orgFinancialHealthIndicator")
      .cache()

    // --- Load annual accounts ----------------------------------------------
    // CUSTOMIZE: replace with your actual annual accounts loader in your codebase.
    // In the KB, annual accounts are typically represented as VAT-level object with `annualAccountSummaries`.
    val annualAccountsPath = inputArgs.getOrElse("annualAccountsPath", "path/to/annual_accounts.parquet")
    val annualAccountsRawDF = sparkSession.read.parquet(annualAccountsPath)

    // Expected (based on KB docs/snippets):
    // - column: vat
    // - column: annualAccountSummaries (array of structs)
    // inside summaries:
    // - periodEndDate
    // - profitLoss.grossMarginO, profitLoss.ebitdaO
    // - balanceSheet.equityO
    // - profitLoss.profitAfterTaxO OR balanceSheet.gainLossPeriodO.currentO (depends on schema)
    // - financialAnalysisO.financialHealthIndicator
    // - numberOfEmployees
    //
    // We explode and derive accountingYear from periodEndDate.

    val explodedAA: DataFrame =
      annualAccountsRawDF
        .select($"vat", explode_outer($"annualAccountSummaries").as("aas"))
        .select(
          $"vat",
          $"aas.periodEndDate".as("periodEndDate"),
          year($"aas.periodEndDate").as("accountingYear"),

          // Equity (KB: balanceSheet.equityO.currentO in some schemas)
          $"aas.balanceSheet.equityO.currentO".as("equity"),

          // Gross margin / EBITDA (KB: profitLoss.grossMarginO, profitLoss.ebitdaO)
          $"aas.profitLoss.grossMarginO".as("grossMargin"),
          $"aas.profitLoss.ebitdaO".as("ebitda"),

          // Profit: try profitAfterTax first; fallback to gainLossPeriod current if present
          coalesce(
            $"aas.profitLoss.profitAfterTaxO",
            $"aas.balanceSheet.gainLossPeriodO.currentO"
          ).as("profit"),

          // Employees
          $"aas.numberOfEmployees".as("employees"),

          // Financial health indicator (annual account-level)
          $"aas.financialAnalysisO.financialHealthIndicator".as("financialHealthIndicator")
        )
        .filter($"accountingYear".between(startYear, endYear))

    // --- Join & export ------------------------------------------------------

    val outDF =
      orgBaseDF
        .join(explodedAA, Seq("vat"), "left_outer")
        .select(
          $"vat",
          $"name",
          $"address",
          $"yearFounded",
          $"accountingYear".as("year"),

          // Prefer annual account indicator if available, else org-level
          coalesce($"financialHealthIndicator", $"orgFinancialHealthIndicator").as("financialHealthIndicator"),

          $"equity",
          $"grossMargin",
          $"profit",
          $"ebitda",
          $"employees"
        )
        .orderBy($"vat", $"year")

    Utils.saveAsCsvFile(
      outDF,
      s"Organizations_NACEBEL_${nacePrefix}_${startYear}_${endYear}.csv"
    )

    orgBaseDF.unpersist()
  }
}
