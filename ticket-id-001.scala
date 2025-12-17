package be.openthebox.modules.adhoc

import be.openthebox.model
import be.openthebox.model.AddressHist
import be.openthebox.modules.{CalcOrganizationAggregates, CalcVatsWithAnnualAccountSummary, Module}
import be.openthebox.util.{InputArgs, Utils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

object ExportOrganizationsNacebel56_2019_2023ToCsv extends Module {

  override def execute(inputArgs: InputArgs)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession.sparkContext.setJobGroup(this.getClass.getName, this.getClass.getName)
    sparkSession.sparkContext.setJobDescription("Export NACEBEL 56 companies (excluding establishments) with 2019-2023 financials")

    // --- helpers -------------------------------------------------------------

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

    // Prefer latest address; if you want "current address", filter endDateO.isEmpty if available
    def lastKnownAddress(org: model.Organization): Option[String] =
      org.addressHists.lastOption.flatMap(createFormattedAddress)

    def companyName(org: model.Organization): String =
      Option(org.nameNl).filter(_.nonEmpty)
        .orElse(Option(org.nameFr).filter(_.nonEmpty))
        .getOrElse("")

    def foundedYear(org: model.Organization): Option[Int] = {
      // CUSTOMIZE: adapt if your org model has different field name (e.g. startDateO / foundingDateO)
      val dOpt: Option[Date] =
        org.startDateO.orElse(org.foundingDateO) // one of these likely exists; keep the one that compiles in your repo
      dOpt.map(d => d.toLocalDate.getYear)
    }

    // Extract a year from java.sql.Date (annual account period end date)
    def yearOf(d: Date): Int = d.toLocalDate.getYear

    // --- load ---------------------------------------------------------------

    val organizationDS = CalcOrganizationAggregates.loadResultsFromParquet
    val annualAccountDS = CalcVatsWithAnnualAccountSummary.loadResultsFromParquet

    // --- filter orgs by NACEBEL 56 -----------------------------------------

    val nace56Companies = organizationDS
      .filter { org =>
        // CUSTOMIZE: if classification/type labels exist, add:
        // org.activities.exists(a => a.classification == "NACEBEL" && a.code.startsWith("56"))
        org.activities.exists(a => Option(a.code).exists(_.startsWith("56")))
      }
      .cache()

    // --- annual accounts -> (vat, year, metrics...) -------------------------

    // We flatten annualAccountSummaries, keep years 2019-2023, then pivot to columns.
    // CUSTOMIZE: adjust field paths if your AnnualAccountSummary model differs.
    val aaYearly = annualAccountDS
      .flatMap { vaa =>
        vaa.annualAccountSummaries.toSeq.flatMap { aas =>
          val y = yearOf(aas.periodEndDate)
          if (y >= 2019 && y <= 2023) {
            Some((
              vaa.vat,
              y,
              // equity
              aas.balanceSheet.equityO.flatMap(_.currentO).map(_.doubleValue()),
              // gross margin
              aas.profitLoss.grossMarginO.map(_.doubleValue()),
              // profit (use gain/loss period if present; else profitAfterTax if your model uses that)
              aas.balanceSheet.gainLossPeriodO.flatMap(_.currentO).map(_.doubleValue())
                .orElse(aas.profitLoss.profitAfterTaxO.map(_.doubleValue())),
              // ebitda
              aas.profitLoss.ebitdaO.map(_.doubleValue())
                .orElse(aas.financialAnalysisO.flatMap(_.ebitda).map(_.doubleValue())),
              // employees
              aas.numberOfEmployees.map(_.toInt),
              // financial health indicator (often in financialAnalysis)
              aas.financialAnalysisO.flatMap(_.financialHealthIndicator).map(_.doubleValue())
            ))
          } else None
        }
      }
      .toDF("vat", "year", "equity", "grossMargin", "profit", "ebitda", "employees", "financialHealthIndicator")

    // Pivot to wide columns per year/metric
    def pivotMetric(df: DataFrame, metricCol: String, outPrefix: String): DataFrame =
      df.groupBy($"vat")
        .pivot($"year", Seq(2019, 2020, 2021, 2022, 2023))
        .agg(first(col(metricCol)))
        .toDF(("vat" +: Seq(2019, 2020, 2021, 2022, 2023).map(y => s"${outPrefix}_${y}")): _*)

    val equityWide     = pivotMetric(aaYearly, "equity", "equity")
    val grossMarginWide= pivotMetric(aaYearly, "grossMargin", "grossMargin")
    val profitWide     = pivotMetric(aaYearly, "profit", "profit")
    val ebitdaWide     = pivotMetric(aaYearly, "ebitda", "ebitda")
    val employeesWide  = pivotMetric(aaYearly, "employees", "employees")

    // Financial health indicator: requirement is not explicitly per-year, but annual accounts have it per year.
    // We'll export it per year as well, and (optionally) a "latest" value.
    val fhiWide        = pivotMetric(aaYearly, "financialHealthIndicator", "financialHealthIndicator")

    // --- build final export --------------------------------------------------

    val baseCompanyDF = nace56Companies
      .map { org =>
        (
          org.vat,
          companyName(org),
          lastKnownAddress(org).getOrElse(""),
          foundedYear(org)
        )
      }
      .toDF("vat", "name", "address", "foundedYear")

    val exportDF =
      baseCompanyDF
        .join(equityWide, Seq("vat"), "left")
        .join(grossMarginWide, Seq("vat"), "left")
        .join(profitWide, Seq("vat"), "left")
        .join(ebitdaWide, Seq("vat"), "left")
        .join(employeesWide, Seq("vat"), "left")
        .join(fhiWide, Seq("vat"), "left")
        .select(
          $"vat",
          $"name",
          $"address",
          $"foundedYear",
          $"financialHealthIndicator_2019", $"financialHealthIndicator_2020", $"financialHealthIndicator_2021", $"financialHealthIndicator_2022", $"financialHealthIndicator_2023",
          $"equity_2019", $"equity_2020", $"equity_2021", $"equity_2022", $"equity_2023",
          $"grossMargin_2019", $"grossMargin_2020", $"grossMargin_2021", $"grossMargin_2022", $"grossMargin_2023",
          $"profit_2019", $"profit_2020", $"profit_2021", $"profit_2022", $"profit_2023",
          $"ebitda_2019", $"ebitda_2020", $"ebitda_2021", $"ebitda_2022", $"ebitda_2023",
          $"employees_2019", $"employees_2020", $"employees_2021", $"employees_2022", $"employees_2023"
        )

    // Write CSV (semicolon separated via Utils.saveAsCsvFile default in this repo)
    Utils.saveAsCsvFile(exportDF, "Organizations_NACEBEL56_2019_2023.csv")

    nace56Companies.unpersist()
  }
}
