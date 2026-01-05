package be.openthebox.modules.adhoc

import be.openthebox.model
import be.openthebox.model.AddressHist
import be.openthebox.modules.{CalcOrganizationAggregates, Module}
import be.openthebox.util.{InputArgs, Utils}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

// NACEBEL 56 (Food & Beverage services) export, org-level only (excluding establishments)
object ExportOrganizationsNacebel56Financials2019_2023ToCsv extends Module {

  override def execute(inputArgs: InputArgs)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    spark.sparkContext.setJobGroup(this.getClass.getName, this.getClass.getName)
    spark.sparkContext.setJobDescription("Export organizations with NACEBEL 56 + financials 2019-2023 to CSV")

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------
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

    def yearFromPeriodEndDate(d: Date): Int = {
      // java.sql.Date#getYear is deprecated, but used in older codebases.
      // If you have java.time in the project, replace with LocalDate conversion.
      d.toLocalDate.getYear
    }

    // ---------------------------------------------------------------------
    // Load data
    // ---------------------------------------------------------------------
    val orgDS = CalcOrganizationAggregates.loadResultsFromParquet

    // IMPORTANT: in this codebase, annual accounts are typically available as
    // "VatWithAnnualAccountSummary" dataset (see knowledge base snippet with convertToAnnualAccountSummaryForExport).
    // CUSTOMIZE: replace the loader below with the actual module in your repo.
    //
    // Example candidates in your project could be:
    // - CalcVatWithAnnualAccountSummary.loadResultsFromParquet
    // - EnrichAnnualAccounts...loadResultsFromParquet
    //
    // For now we load it via Spark read as a placeholder (will not compile until replaced).
    val vatWithAnnualAccountSummaryDS =
      spark.read.parquet(inputArgs.inputPath() + "/vat_with_annual_account_summary")
        .as[model.VatWithAnnualAccountSummary] // CUSTOMIZE: adjust package/type if different

    // ---------------------------------------------------------------------
    // Filter: NACEBEL 56 (organization-level only)
    // ---------------------------------------------------------------------
    val filteredOrgs: Dataset[model.OrganisationBO] =
      orgDS.filter { org =>
        // NACEBEL codes are strings like "56", "56.10", etc.
        org.activities.exists(a =>
          a.classification == "NACEBEL" && Option(a.code).exists(_.startsWith("56"))
        )
      }

    // ---------------------------------------------------------------------
    // Enrich with financials 2019-2023
    // ---------------------------------------------------------------------
    val years = 2019 to 2023

    val resultDF =
      filteredOrgs
        .joinWith(
          vatWithAnnualAccountSummaryDS,
          filteredOrgs("vat") === vatWithAnnualAccountSummaryDS("vat"),
          "left_outer"
        )
        .map { case (org, annualOrNull) =>
          val annualO = Option(annualOrNull)

          val lastAddress = org.addressHists.lastOption.flatMap(createFormattedAddress)

          // Best effort founded year:
          // CUSTOMIZE: if your org model uses a different field name (e.g. startDate/foundingDate)
          val foundedYear: Option[Int] =
            org.startDateO.map(_.toLocalDate.getYear) // CUSTOMIZE if needed

          // For each requested year, pull metrics from the AnnualAccountSummary with matching periodEndDate year
          def byYear[A](f: model.AnnualAccountSummary => Option[A]): Map[Int, Option[A]] = {
            val summaries = annualO.map(_.annualAccountSummaries.toSeq).getOrElse(Seq.empty)
            val byYr = summaries.groupBy(s => yearFromPeriodEndDate(s.periodEndDate))

            years.map { y =>
              val bestSummary = byYr.getOrElse(y, Seq.empty)
                .sortBy(_.publicationDate.getTime) // choose latest if multiple
                .lastOption

              y -> bestSummary.flatMap(f)
            }.toMap
          }

          // Field mapping per knowledge base snippet:
          // - grossMarginO (ProfitLoss)
          // - profitAfterTaxO (ProfitLoss) OR gainLossPeriodO (BalanceSheet) depending on your schema
          // - ebitdaO (ProfitLoss) in the reference schema, but some implementations use financialAnalysisO.ebitda
          // - equity current (BalanceSheet.equityO.currentO) in some schemas, or BalanceSheet.equityO directly in others
          //
          // CUSTOMIZE: adjust these extractors to match your actual AnnualAccountSummary structure.
          val equityByYear = byYear[Double](aas => aas.balanceSheet.equityO.flatMap(_.currentO))
          val grossMarginByYear = byYear[Double](aas => aas.profitLoss.grossMarginO)
          val profitByYear = byYear[Double](aas => aas.profitLoss.profitAfterTaxO)
          val ebitdaByYear = byYear[Double](aas => aas.profitLoss.ebitdaO)
          val employeesByYear = byYear[Int](aas => aas.numberOfEmployees)

          val financialHealthByYear =
            byYear[Double](aas => aas.financialAnalysisO.flatMap(_.financialHealthIndicator)) // CUSTOMIZE if field differs

          OutputRow(
            vat = org.vat,
            name = org.nameNl.orElse(org.nameFr).getOrElse(""),
            address = lastAddress.getOrElse(""),
            foundedYear = foundedYear,
            // flatten year metrics into columns for CSV
            fhi2019 = financialHealthByYear(2019),
            fhi2020 = financialHealthByYear(2020),
            fhi2021 = financialHealthByYear(2021),
            fhi2022 = financialHealthByYear(2022),
            fhi2023 = financialHealthByYear(2023),

            equity2019 = equityByYear(2019),
            equity2020 = equityByYear(2020),
            equity2021 = equityByYear(2021),
            equity2022 = equityByYear(2022),
            equity2023 = equityByYear(2023),

            grossMargin2019 = grossMarginByYear(2019),
            grossMargin2020 = grossMarginByYear(2020),
            grossMargin2021 = grossMarginByYear(2021),
            grossMargin2022 = grossMarginByYear(2022),
            grossMargin2023 = grossMarginByYear(2023),

            profit2019 = profitByYear(2019),
            profit2020 = profitByYear(2020),
            profit2021 = profitByYear(2021),
            profit2022 = profitByYear(2022),
            profit2023 = profitByYear(2023),

            ebitda2019 = ebitdaByYear(2019),
            ebitda2020 = ebitdaByYear(2020),
            ebitda2021 = ebitdaByYear(2021),
            ebitda2022 = ebitdaByYear(2022),
            ebitda2023 = ebitdaByYear(2023),

            employees2019 = employeesByYear(2019),
            employees2020 = employeesByYear(2020),
            employees2021 = employeesByYear(2021),
            employees2022 = employeesByYear(2022),
            employees2023 = employeesByYear(2023)
          )
        }
        .toDF()

    // ---------------------------------------------------------------------
    // Write CSV
    // ---------------------------------------------------------------------
    Utils.saveAsCsvFile(
      resultDF,
      "Organizations_NACEBEL56_Financials_2019_2023.csv"
    )
  }

  // Output schema: one row per company (organization), with year columns 2019-2023
  case class OutputRow(
    vat: String,
    name: String,
    address: String,
    foundedYear: Option[Int],

    fhi2019: Option[Double],
    fhi2020: Option[Double],
    fhi2021: Option[Double],
    fhi2022: Option[Double],
    fhi2023: Option[Double],

    equity2019: Option[Double],
    equity2020: Option[Double],
    equity2021: Option[Double],
    equity2022: Option[Double],
    equity2023: Option[Double],

    grossMargin2019: Option[Double],
    grossMargin2020: Option[Double],
    grossMargin2021: Option[Double],
    grossMargin2022: Option[Double],
    grossMargin2023: Option[Double],

    profit2019: Option[Double],
    profit2020: Option[Double],
    profit2021: Option[Double],
    profit2022: Option[Double],
    profit2023: Option[Double],

    ebitda2019: Option[Double],
    ebitda2020: Option[Double],
    ebitda2021: Option[Double],
    ebitda2022: Option[Double],
    ebitda2023: Option[Double],

    employees2019: Option[Int],
    employees2020: Option[Int],
    employees2021: Option[Int],
    employees2022: Option[Int],
    employees2023: Option[Int]
  )
}
