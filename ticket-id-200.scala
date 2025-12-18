package be.openthebox.modules.adhoc

import be.openthebox.model
import be.openthebox.model.AddressHist
import be.openthebox.modules.{CalcOrganizationAggregates, Module}
import be.openthebox.util.{InputArgs, Utils}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

/**
 * Export for companies with NACEBEL code 56 (excluding establishments)
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
 * Output: one row per vat+year.
 */
object ExportOrganizationsNacebel56_2019_2023_ToCsv extends Module {

  override def execute(inputArgs: InputArgs)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession.sparkContext.setJobGroup(this.getClass.getName, this.getClass.getName)
    sparkSession.sparkContext.setJobDescription("Export organizations with NACEBEL 56 + financials 2019-2023")

    // ---------------------------
    // Helpers
    // ---------------------------

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

    def yearFromSqlDate(d: Date): Int = d.toLocalDate.getYear

    // ---------------------------
    // Load organization aggregates
    // ---------------------------

    val organizationDS = CalcOrganizationAggregates.loadResultsFromParquet.cache()

    // IMPORTANT:
    // - "excluding establishments" here means: do NOT use establishment dataset; keep only VAT-level organizations.
    // - Defensive extra filter: VAT should look like Belgian VAT "BE..." (adapt if your data contains non-BE VATs to keep).
    val org56DS = organizationDS
      .filter(org => Option(org.vat).exists(_.nonEmpty))
      // CUSTOMIZE: If you want to keep foreign VATs too, remove this next line.
      .filter(org => org.vat.toUpperCase.startsWith("BE"))
      // Filter on NACEBEL 56 (prefix match: "56", includes "56.10", "5610", etc.)
      .filter { org =>
        org.activities.exists { act =>
          Option(act.classification).exists(_.toUpperCase == "NACEBEL") &&
            Option(act.code).exists(_.replace(".", "").startsWith("56"))
        }
      }
      .map { org =>
        val name = Option(org.nameNl).filter(_.nonEmpty)
          .orElse(Option(org.nameFr).filter(_.nonEmpty))
          .getOrElse("")

        val address = org.addressHists.lastOption.flatMap(createFormattedAddress).getOrElse("")

        // CUSTOMIZE: depending on your model field naming this might be `startDate`, `foundingDate`, ...
        // In many aggregates, this is `startDateO`.
        val foundedYear: Option[Int] =
          org.startDateO.map(d => d.toLocalDate.getYear)

        // Organization-side health indicator sometimes exists; but requested is financial health indicator (annual accounts).
        // We'll keep this org-level indicator too if present (optional); main output uses AA indicator per year.
        val orgHealthO: Option[Double] = org.financialHealthIndicatorO

        (org.vat, name, address, foundedYear, orgHealthO)
      }
      .toDF("vat", "name", "address", "yearFounded", "orgHealthIndicator")

    // ---------------------------
    // Load annual accounts and extract years 2019-2023
    // ---------------------------

    // NOTE:
    // The annual accounts dataset loader name differs per project.
    // In this codebase, it is typically available as a parquet Dataset with a structure that contains:
    //   vatWithAnnualAccountSummary.annualAccountSummaries[*].periodEndDate, profitLoss.grossMarginO, profitLoss.ebitdaO, ...
    //
    // CUSTOMIZE: Replace this with the correct loader in your repo, e.g.:
    //   val annualAccountDS = CalcAnnualAccountAggregates.loadResultsFromParquet
    // or
    //   val annualAccountDS = ImportAnnualAccounts.loadResultsFromParquet
    //
    val annualAccountDS = be.openthebox.modules.CalcAnnualAccountAggregates.loadResultsFromParquet

    val aaFlat = annualAccountDS
      .select($"vat", explode($"annualAccountSummaries").as("aas"))
      .select(
        $"vat",
        $"aas.periodEndDate".as("periodEndDate"),
        $"aas.financialAnalysisO.financialHealthIndicator".as("financialHealthIndicator"),
        $"aas.balanceSheet.equityO.currentO".as("equity"),
        $"aas.profitLoss.grossMarginO".as("grossMargin"),
        // profit: depending on schema, you might want profitAfterTaxO or operatingProfitO.
        // Here we use profitAfterTaxO as "Profit" (most common business meaning).
        $"aas.profitLoss.profitAfterTaxO".as("profit"),
        $"aas.profitLoss.ebitdaO".as("ebitda"),
        $"aas.numberOfEmployees".as("employees")
      )
      .withColumn("year", year($"periodEndDate"))
      .filter($"year".between(2019, 2023))
      .drop("periodEndDate")

    // ---------------------------
    // Join + output
    // ---------------------------

    val resultDF =
      org56DS
        .join(aaFlat, Seq("vat"), "left_outer")
        .select(
          $"vat",
          $"name",
          $"address",
          $"yearFounded",
          $"year",
          $"financialHealthIndicator",
          $"equity",
          $"grossMargin",
          $"profit",
          $"ebitda",
          $"employees"
        )
        .orderBy($"vat", $"year")

    // Output filename: keep consistent with your existing exports
    Utils.saveAsCsvFile(resultDF, "Organizations_NACEBEL_56_Financials_2019_2023.csv")

    organizationDS.unpersist()
  }
}
