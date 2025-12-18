package be.openthebox.modules.adhoc

import be.openthebox.model
import be.openthebox.model.AddressHist
import be.openthebox.modules.{CalcOrganizationAggregates, CalcVatsWithAnnualAccountSummary, Module}
import be.openthebox.util.{InputArgs, Utils}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

/**
 * Export companies with NACEBEL code 56 (excluding establishments)
 * Fields required for years 2019-2023:
 * - Name
 * - Address
 * - Year founded
 * - Financial health indicator
 * - Equity
 * - Gross margin
 * - Profit
 * - EBITDA
 * - Employees
 */
object ExportOrganizationsNacebel56_2019_2023_ToCsv extends Module {

  override def execute(inputArgs: InputArgs)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    spark.sparkContext.setJobGroup(this.getClass.getName, this.getClass.getName)
    spark.sparkContext.setJobDescription("Export NACEBEL 56 companies with financials (2019-2023)")

    val startYear = 2019
    val endYear = 2023

    // Same address formatting helper as used in ExportOrganizationsCustom7CountryExportToCsv.scala
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

    // --- Load sources
    val organizationDS = CalcOrganizationAggregates.loadResultsFromParquet

    // Annual accounts: dataset with (vat -> annualAccountSummaries[])
    // CUSTOMIZE: if your module name differs, adapt accordingly.
    val vatWithAnnualAccountSummaryDS = CalcVatsWithAnnualAccountSummary.loadResultsFromParquet

    // --- Filter organizations by NACEBEL 56 (company-level, no establishments involved)
    val nace56CompaniesDF =
      organizationDS
        .filter { org =>
          org.activities.exists { act =>
            // defensive: some datasets contain classification labels
            val isNacebel = Option(act.classification).forall(_.toUpperCase.contains("NACE"))
            isNacebel && Option(act.code).exists(_.startsWith("56"))
          }
        }
        .map { org =>
          val name = Option(org.nameNl).filter(_.nonEmpty)
            .orElse(Option(org.nameFr).filter(_.nonEmpty))
            .getOrElse("")

          val address = org.addressHists.lastOption.flatMap(createFormattedAddress).getOrElse("")

          // CUSTOMIZE: founding date / year field name might differ depending on the BO class version.
          // Try org.startDateO / org.foundingDateO / org.yearFoundedO in your model.
          val yearFounded: Option[Int] =
            org.startDateO.map(d => d.toLocalDate.getYear)
              .orElse(org.foundingDateO.map(d => d.toLocalDate.getYear))
              .orElse(org.yearFoundedO)

          val financialHealthIndicator: Option[Double] =
            org.financialAnalysisO.flatMap(_.financialHealthIndicator)

          (org.vat, name, address, yearFounded, financialHealthIndicator)
        }
        .toDF("vat", "name", "address", "yearFounded", "financialHealthIndicator")
        .cache()

    // --- Flatten annual accounts to (vat, year, metrics...)
    val annualsFlatDF =
      vatWithAnnualAccountSummaryDS
        .select($"vat", explode($"annualAccountSummaries").as("aas"))
        .select(
          $"vat",
          $"aas.periodEndDate".as("periodEndDate"),
          year($"aas.periodEndDate").as("year"),

          // Equity: try common locations (depends on model version)
          $"aas.balanceSheet.equityO.currentO".as("equity"),

          // Profit: if your model has profitAfterTaxO use that, otherwise fallback to balanceSheet gain/loss
          $"aas.profitLoss.profitAfterTaxO".as("profit_pl"),
          $"aas.balanceSheet.gainLossPeriodO.currentO".as("profit_bs"),

          $"aas.profitLoss.grossMarginO".as("grossMargin"),
          $"aas.profitLoss.ebitdaO".as("ebitda"),
          $"aas.numberOfEmployees".as("employees")
        )
        .withColumn(
          "profit",
          coalesce(col("profit_pl"), col("profit_bs"))
        )
        .drop("profit_pl", "profit_bs")
        .filter($"year".between(startYear, endYear))

    // --- Join + select output
    val exportDF =
      nace56CompaniesDF
        .join(annualsFlatDF, Seq("vat"), "left_outer")
        .select(
          $"vat",
          $"name",
          $"address",
          $"yearFounded",
          $"financialHealthIndicator",
          $"year",
          $"equity",
          $"grossMargin",
          $"profit",
          $"ebitda",
          $"employees"
        )
        // Optional: keep only rows where a year exists (i.e., has annual accounts in range)
        // .filter($"year".isNotNull)
        .orderBy($"vat", $"year")

    // --- Write CSV
    // CUSTOMIZE: output filename if needed
    Utils.saveAsCsvFile(exportDF, s"Organizations_NACEBEL56_${startYear}_${endYear}.csv")

    nace56CompaniesDF.unpersist()
  }
}
