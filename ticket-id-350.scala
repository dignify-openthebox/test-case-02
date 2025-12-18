package be.openthebox.modules.adhoc

import be.openthebox.model
import be.openthebox.model.{AddressHist, AnnualAccountSummary, OrganisationBO, VatWithAnnualAccountSummary}
import be.openthebox.modules.{CalcOrganizationAggregates, CalcVatsWithAnnualAccountSummary, Module}
import be.openthebox.util.{InputArgs, Utils}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Date

object ExportOrganizationsNacebel56CompaniesFinancials2019_2023ToCsv extends Module {

  override def execute(inputArgs: InputArgs)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession.sparkContext.setJobGroup(this.getClass.getName, this.getClass.getName)
    sparkSession.sparkContext.setJobDescription("Export NACEBEL 56 companies (excluding establishments) with financials 2019-2023 to CSV")

    // -------------------------------------------------------------------------
    // Helpers (copied from ExportOrganizationsCustom7CountryExportToCsv.scala style)
    // -------------------------------------------------------------------------
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

    def companyName(org: OrganisationBO): String =
      Option(org.nameNl).filter(_.trim.nonEmpty)
        .orElse(Option(org.nameFr).filter(_.trim.nonEmpty))
        .getOrElse("")

    def foundedYear(org: OrganisationBO): Option[Int] = {
      // CUSTOMIZE: adjust if your model uses a different field than startDateO/foundingDateO
      // We keep it defensive because schemas differ between versions.
      val d: Option[Date] =
        org.startDateO
          .orElse(org.foundingDateO) // if exists in your model
          .orElse(org.creationDateO) // if exists in your model

      d.map(dt => 1900 + dt.getYear) // java.sql.Date#getYear is offset from 1900
    }

    def hasNacebel56(org: OrganisationBO): Boolean =
      org.activities.exists(a =>
        Option(a.classification).exists(_.toUpperCase == "NACEBEL") &&
          Option(a.code).exists(_.startsWith("56"))
      )

    // -------------------------------------------------------------------------
    // Load
    // -------------------------------------------------------------------------
    val orgDS = CalcOrganizationAggregates.loadResultsFromParquet
    val vatWithAA: org.apache.spark.sql.Dataset[VatWithAnnualAccountSummary] =
      CalcVatsWithAnnualAccountSummary.loadResultsFromParquet

    // -------------------------------------------------------------------------
    // Filter companies on NACEBEL 56
    // NOTE: "excluding establishments" is satisfied by exporting ONLY organizations,
    //       not joining/expanding establishment dataset.
    // -------------------------------------------------------------------------
    val nace56Companies = orgDS
      .filter(hasNacebel56)
      .map { org =>
        val lastAddr = org.addressHists.lastOption.flatMap(createFormattedAddress)
        (org.vat, companyName(org), lastAddr, foundedYear(org), org.financialHealthIndicatorO)
      }
      .toDF("vat", "name", "address", "yearFounded", "financialHealthIndicator")

    // -------------------------------------------------------------------------
    // Flatten annual accounts to (vat, year, metrics) for 2019-2023
    // -------------------------------------------------------------------------
    val aa2019_2023 = vatWithAA
      .flatMap { v =>
        v.annualAccountSummaries.toSeq.flatMap { aas: AnnualAccountSummary =>
          val year = 1900 + aas.periodEndDate.getYear
          if (year >= 2019 && year <= 2023) {
            // Field mapping based on KB snippets:
            // - equity from balanceSheet.equityO.currentO
            // - profit from balanceSheet.gainLossPeriodO.currentO (common in this project)
            // - ebitda from financialAnalysisO.ebitda
            // - gross margin from profitLoss.grossMarginO (if your schema has it there)
            // - employees from numberOfEmployees
            val equityO = aas.balanceSheet.equityO.flatMap(_.currentO)
            val profitO = aas.balanceSheet.gainLossPeriodO.flatMap(_.currentO)
            val ebitdaO = aas.financialAnalysisO.flatMap(_.ebitda)

            // CUSTOMIZE: depending on your model, gross margin may be:
            // aas.profitLoss.grossMarginO  OR aas.financialAnalysisO.flatMap(_.grossMargin) OR elsewhere
            val grossMarginO = aas.profitLoss.grossMarginO

            val employeesO = aas.numberOfEmployees

            Some((v.vat, year, equityO, grossMarginO, profitO, ebitdaO, employeesO))
          } else None
        }
      }
      .toDF("vat", "year", "equity", "grossMargin", "profit", "ebitda", "employees")

    // -------------------------------------------------------------------------
    // Join and export (one row per vat-year)
    // -------------------------------------------------------------------------
    val out: DataFrame = nace56Companies
      .join(aa2019_2023, Seq("vat"), "left_outer")
      .select(
        $"vat",
        $"year",
        $"name",
        $"address",
        $"yearFounded",
        $"financialHealthIndicator",
        $"equity",
        $"grossMargin",
        $"profit",
        $"ebitda",
        $"employees"
      )

    Utils.saveAsCsvFile(
      out,
      "Organizations_NACEBEL56_Financials_2019_2023.csv"
    )
  }

}
