package be.openthebox.modules.adhoc

import be.openthebox.model
import be.openthebox.model.AddressHist
import be.openthebox.modules.{CalcOrganizationAggregates, Module}
import be.openthebox.util.{InputArgs, Utils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Date

/**
 * Export companies with NACEBEL code 56 (excluding establishments) with financials for 2019–2023.
 *
 * Output: Organizations_NACE56_2019_2023.csv
 *
 * Notes:
 * - "Excluding establishments": we only export organizations (CalcOrganizationAggregates) and do not explode/join establishment dataset.
 * - One row per VAT per accounting year.
 */
object ExportOrganizationsNace56_2019_2023_ToCsv extends Module {

  override def execute(inputArgs: InputArgs)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession.sparkContext.setJobGroup(this.getClass.getName, this.getClass.getName)
    sparkSession.sparkContext.setJobDescription("Export NACEBEL 56 organizations with financials 2019–2023")

    // --- Helpers (copied from existing custom exports style) -------------------
    def createFormattedAddress(addressHist: AddressHist): Option[String] = {
      val address: Option[model.Address] = addressHist.addressNlO.orElse(addressHist.addressFrO)
      address.map { ad =>
        val elements =
          List(Some(ad.number), Some(ad.street), Some(ad.zipCode), Some(ad.city), ad.provinceO, Some(ad.countryCode))
        elements.flatten.map(_.toString).mkString(",")
      }
    }

    def orgName(org: model.Organization): String =
      Option(org.nameNl).filter(_.nonEmpty)
        .orElse(Option(org.nameFr).filter(_.nonEmpty))
        .getOrElse("")

    // --- Load organizations ----------------------------------------------------
    val orgDS = CalcOrganizationAggregates.loadResultsFromParquet

    // Filter: NACEBEL 56 (Food & beverage services)
    // CUSTOMIZE: if your activity structure differs, adjust this predicate
    val nace56OrgDS = orgDS.filter { org =>
      org.activities.exists { act =>
        // common patterns: classification == "NACEBEL", code like "56", "56.10", ...
        val isNacebel = Option(act.classification).exists(_.toUpperCase.contains("NACE"))
        val is56 = Option(act.code).exists(_.startsWith("56"))
        isNacebel && is56
      }
    }

    // --- Load annual accounts and flatten 2019–2023 ---------------------------
    // CUSTOMIZE: replace with your real loader if different in your repo
    // The KB shows AnnualAccountSummary has: periodEndDate, balanceSheet.equityO.currentO, profitLoss.grossMarginO, etc.
    val annualAccountDS = sparkSession.read.parquet(inputArgs.input() + "/annual_accounts").as[model.VAT_AnnualAccount]

    // Flatten to (vat, year, equity, grossMargin, profit, ebitda, employees)
    val fin2019_2023 = annualAccountDS
      .flatMap { aa =>
        aa.annualAccountSummaries.toSeq.flatMap { s =>
          val year = s.periodEndDate.toLocalDate.getYear
          if (year >= 2019 && year <= 2023) {
            val equity = s.balanceSheet.equityO.flatMap(_.currentO)
            val grossMargin = s.profitLoss.grossMarginO
            // IMPORTANT: depending on schema you might want profitAfterTaxO, profitBeforeTaxO, or gainLossPeriodO.
            // Here we use "profitAfterTaxO" when available, otherwise fallback to operating profit if present.
            val profit =
              s.profitLoss.profitAfterTaxO
                .orElse(s.profitLoss.operatingProfitO)

            val ebitda = s.profitLoss.ebitdaO.orElse(s.financialAnalysisO.flatMap(_.ebitda))
            val employees = s.numberOfEmployees

            Some((aa.vat, year, equity, grossMargin, profit, ebitda, employees))
          } else None
        }
      }
      .toDF("vat", "year", "equity", "grossMargin", "profit", "ebitda", "employees")

    // --- Organization base fields ---------------------------------------------
    val orgBaseDF: DataFrame =
      nace56OrgDS
        .map { org =>
          val lastAddress = org.addressHists.lastOption.flatMap(createFormattedAddress)
          (
            org.vat,
            orgName(org),
            lastAddress.getOrElse(""),
            org.startDateO.map(_.toString).getOrElse(""),          // year founded (date)
            org.financialHealthIndicatorO.map(_.toString).getOrElse("") // financial health indicator
          )
        }
        .toDF("vat", "name", "address", "foundedDate", "financialHealthIndicator")
        .dropDuplicates("vat")

    // --- Join orgs + financials (one row per year) ----------------------------
    val outDF =
      orgBaseDF
        .join(fin2019_2023, Seq("vat"), "left")
        .select(
          $"vat",
          $"name",
          $"address",
          $"foundedDate".alias("yearFounded"),
          $"financialHealthIndicator",
          $"year",
          $"equity",
          $"grossMargin",
          $"profit",
          $"ebitda",
          $"employees"
        )
        .orderBy($"vat", $"year")

    // --- Write ----------------------------------------------------------------
    Utils.saveAsCsvFile(
      outDF,
      "Organizations_NACE56_2019_2023.csv"
    )
  }
}
