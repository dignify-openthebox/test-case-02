package be.openthebox.modules.adhoc

import be.openthebox.model
import be.openthebox.model.AddressHist
import be.openthebox.modules.{CalcOrganizationAggregates, CalcVatsWithAnnualAccountSummary, Module}
import be.openthebox.util.{InputArgs, Utils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ExportNacebel56CompaniesFinancials2019_2023ToCsv extends Module {

  override def execute(inputArgs: InputArgs)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    spark.sparkContext.setJobGroup(this.getClass.getName, this.getClass.getName)
    spark.sparkContext.setJobDescription("Export NACEBEL 56 companies with financials 2019-2023 (excluding establishments)")

    // ----------------------------
    // Helpers
    // ----------------------------
    def formattedAddressFromHist(ah: AddressHist): Option[String] = {
      val address: Option[model.Address] = ah.addressNlO.orElse(ah.addressFrO)
      address.map { ad =>
        val parts = List(
          Option(ad.street),
          Option(ad.number),
          Option(ad.zipCode),
          Option(ad.city),
          ad.provinceO,
          Option(ad.countryCode)
        ).flatten.map(_.toString.trim).filter(_.nonEmpty)
        parts.mkString(", ")
      }
    }

    def orgDisplayName(org: model.OrganisationBO): String =
      org.nameNlO.orElse(org.nameFrO).getOrElse("")

    // ----------------------------
    // Load datasets
    // ----------------------------
    val orgDS = CalcOrganizationAggregates.loadResultsFromParquet

    // VatWithAnnualAccountSummary dataset
    val vatWithAA = CalcVatsWithAnnualAccountSummary.loadResultsFromParquet

    // ----------------------------
    // 1) Filter orgs by NACEBEL 56
    // ----------------------------
    val nacebel56Orgs = orgDS.filter { org =>
      org.activities.exists(a =>
        a.classification == "NACEBEL" && a.code != null && a.code.startsWith("56")
      )
    }

    // Take last known address from organization address history
    val orgBaseDF: DataFrame =
      nacebel56Orgs
        .map { org =>
          val lastAddr = org.addressHists.lastOption.flatMap(formattedAddressFromHist)
          (
            org.vat,
            orgDisplayName(org),
            lastAddr.getOrElse(""),
            org.startDateO.map(_.toString).getOrElse(""),              // "Year founded" basis; customize below if you want only year
            org.financialHealthIndicatorO.map(_.toString).getOrElse("") // org-level indicator if present
          )
        }
        .toDF("vat", "name", "address", "foundedDate", "financialHealthIndicator_org")

    // ----------------------------
    // 2) Extract annual accounts 2019-2023 to (vat, year, metrics...)
    // ----------------------------
    val aa2019_2023DF: DataFrame =
      vatWithAA
        .flatMap { v =>
          v.annualAccountSummaries.flatMap { aas =>
            val year = Option(aas.periodEndDate).map(d => d.toLocalDate.getYear)
            year.filter(y => y >= 2019 && y <= 2023).map { y =>
              val equity =
                aas.balanceSheet.equityO.flatMap(_.currentO).map(_.toDouble)

              val grossMargin =
                aas.profitLoss.grossMarginO.map(_.toDouble)

              // Profit: using gainLossPeriod if available (common in examples),
              // otherwise fallback to profitAfterTax if present.
              val profit =
                aas.balanceSheet.gainLossPeriodO.flatMap(_.currentO).map(_.toDouble)
                  .orElse(aas.profitLoss.profitAfterTaxO.map(_.toDouble))

              // EBITDA: in some schemas it sits in financialAnalysisO; in others in profitLoss.ebitdaO
              val ebitda =
                aas.financialAnalysisO.flatMap(_.ebitda).map(_.toDouble)
                  .orElse(aas.profitLoss.ebitdaO.map(_.toDouble))

              val employees = aas.numberOfEmployees

              val finHealth =
                aas.financialAnalysisO.flatMap(_.financialHealthIndicator).map(_.toString)

              (
                v.vat,
                y,
                finHealth.getOrElse(""),
                equity.orNull,
                grossMargin.orNull,
                profit.orNull,
                ebitda.orNull,
                employees.orNull
              )
            }
          }
        }
        .toDF(
          "vat",
          "year",
          "financialHealthIndicator",
          "equity",
          "grossMargin",
          "profit",
          "ebitda",
          "employees"
        )

    // ----------------------------
    // 3) Join and export
    // ----------------------------
    val outDF =
      orgBaseDF
        .join(aa2019_2023DF, Seq("vat"), "left_outer")
        .select(
          $"vat",
          $"name",
          $"address",
          $"foundedDate",
          // Prefer annual-account health indicator if present, else org-level one
          when(length(trim($"financialHealthIndicator")) > 0, $"financialHealthIndicator")
            .otherwise($"financialHealthIndicator_org")
            .as("financialHealthIndicator"),
          $"year",
          $"equity",
          $"grossMargin",
          $"profit",
          $"ebitda",
          $"employees"
        )
        .orderBy($"vat", $"year")

    // CUSTOMIZE: output path / filename strategy
    Utils.saveAsCsvFile(outDF, "NACEBEL56_companies_financials_2019_2023.csv")
  }
}
