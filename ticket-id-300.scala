package be.openthebox.modules.adhoc

import be.openthebox.model
import be.openthebox.model.{AddressHist, Organization, VatWithAnnualAccountSummary}
import be.openthebox.modules.{CalcOrganizationAggregates, CalcVatsWithAnnualAccountSummary, Module}
import be.openthebox.util.{InputArgs, Utils}
import org.apache.spark.sql.{Dataset, SparkSession}

import java.sql.Date

object ExportOrganizationsNacebel56_NoEstablishments_2019_2023_ToCsv extends Module {

  override def execute(inputArgs: InputArgs)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession.sparkContext.setJobGroup(this.getClass.getName, this.getClass.getName)
    sparkSession.sparkContext.setJobDescription("Export organizations NACEBEL 56 (no establishments) with financials 2019-2023")

    // Same helper pattern as ExportOrganizationsCustom7CountryExportToCsv.scala
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

    def yearFromEndDate(d: Date): Int = d.toLocalDate.getYear

    // Load datasets
    val organizationDS: Dataset[Organization] =
      CalcOrganizationAggregates.loadResultsFromParquet

    val vatWithAnnualAccountSummaryDS: Dataset[VatWithAnnualAccountSummary] =
      CalcVatsWithAnnualAccountSummary.loadResultsFromParquet

    // --- 1) Filter: NACEBEL code 56 (organizations only; no establishments exported) ---
    val nace56OrgsDS = organizationDS.filter { org =>
      org.activities.exists { act =>
        // CUSTOMIZE: if your model has classification/typeLabel, you can tighten the filter
        // e.g. act.classification == "NACEBEL"
        Option(act.code).exists(_.startsWith("56"))
      }
    }.cache()

    // --- 2) Preprocess annual accounts into per-(vat,year) metrics for 2019-2023 ---
    // We explode annualAccountSummaries on the typed side (flatMap) to keep it simple & null-safe.
    val fin2019_2023 = vatWithAnnualAccountSummaryDS
      .flatMap { vaa =>
        vaa.annualAccountSummaries
          .filter(aas => {
            val y = yearFromEndDate(aas.periodEndDate)
            y >= 2019 && y <= 2023
          })
          .map { aas =>
            val y = yearFromEndDate(aas.periodEndDate)

            // NOTE: Field locations vary per schema version; these follow the DATA_MODEL_REFERENCE snippets.
            val equityO: Option[Double] =
              aas.balanceSheet.equityO.flatMap(_.currentO)

            val grossMarginO: Option[Double] =
              aas.profitLoss.grossMarginO

            val profitO: Option[Double] =
              // profit can be represented in multiple ways; adjust if needed
              aas.profitLoss.profitAfterTaxO
                .orElse(aas.profitLoss.operatingProfitO)

            val ebitdaO: Option[Double] =
              aas.profitLoss.ebitdaO
                .orElse(aas.financialAnalysisO.flatMap(_.ebitda))

            val employeesO: Option[Int] =
              aas.numberOfEmployees

            val financialHealthIndicatorO: Option[Double] =
              aas.financialAnalysisO.flatMap(_.financialHealthIndicator)

            AnnualRow(
              vat = vaa.vat,
              year = y,
              financialHealthIndicator = financialHealthIndicatorO,
              equity = equityO,
              grossMargin = grossMarginO,
              profit = profitO,
              ebitda = ebitdaO,
              employees = employeesO
            )
          }
      }
      .groupByKey(r => (r.vat, r.year))
      .reduceGroups { (a, b) =>
        // if duplicates exist, keep "most complete"
        def score(r: AnnualRow): Int =
          List(
            r.financialHealthIndicator,
            r.equity,
            r.grossMargin,
            r.profit,
            r.ebitda
          ).count(_.isDefined) + (if (r.employees.isDefined) 1 else 0)

        if (score(a) >= score(b)) a else b
      }
      .map(_._2)
      .cache()

    // Create a map-like wide table per VAT: columns for 2019..2023
    val finWideByVat = fin2019_2023
      .groupByKey(_.vat)
      .mapGroups { case (vat, rowsIt) =>
        val rows = rowsIt.toList
        def byYear(y: Int): Option[AnnualRow] = rows.find(_.year == y)

        FinancialsWide(
          vat = vat,

          fhi2019 = byYear(2019).flatMap(_.financialHealthIndicator),
          equity2019 = byYear(2019).flatMap(_.equity),
          grossMargin2019 = byYear(2019).flatMap(_.grossMargin),
          profit2019 = byYear(2019).flatMap(_.profit),
          ebitda2019 = byYear(2019).flatMap(_.ebitda),
          employees2019 = byYear(2019).flatMap(_.employees),

          fhi2020 = byYear(2020).flatMap(_.financialHealthIndicator),
          equity2020 = byYear(2020).flatMap(_.equity),
          grossMargin2020 = byYear(2020).flatMap(_.grossMargin),
          profit2020 = byYear(2020).flatMap(_.profit),
          ebitda2020 = byYear(2020).flatMap(_.ebitda),
          employees2020 = byYear(2020).flatMap(_.employees),

          fhi2021 = byYear(2021).flatMap(_.financialHealthIndicator),
          equity2021 = byYear(2021).flatMap(_.equity),
          grossMargin2021 = byYear(2021).flatMap(_.grossMargin),
          profit2021 = byYear(2021).flatMap(_.profit),
          ebitda2021 = byYear(2021).flatMap(_.ebitda),
          employees2021 = byYear(2021).flatMap(_.employees),

          fhi2022 = byYear(2022).flatMap(_.financialHealthIndicator),
          equity2022 = byYear(2022).flatMap(_.equity),
          grossMargin2022 = byYear(2022).flatMap(_.grossMargin),
          profit2022 = byYear(2022).flatMap(_.profit),
          ebitda2022 = byYear(2022).flatMap(_.ebitda),
          employees2022 = byYear(2022).flatMap(_.employees),

          fhi2023 = byYear(2023).flatMap(_.financialHealthIndicator),
          equity2023 = byYear(2023).flatMap(_.equity),
          grossMargin2023 = byYear(2023).flatMap(_.grossMargin),
          profit2023 = byYear(2023).flatMap(_.profit),
          ebitda2023 = byYear(2023).flatMap(_.ebitda),
          employees2023 = byYear(2023).flatMap(_.employees)
        )
      }

    // --- 3) Enrich orgs with name/address/founded + join financials wide ---
    val exportDF =
      nace56OrgsDS
        .map { org =>
          val name = Option(org.nameNl).filter(_.nonEmpty)
            .orElse(Option(org.nameFr).filter(_.nonEmpty))
            .getOrElse("")

          val address = org.addressHists.lastOption.flatMap(createFormattedAddress).getOrElse("")

          ExportOrgBase(
            vat = org.vat,
            name = name,
            address = address,
            yearFounded = Option(org.startDate).map(_.toLocalDate.getYear)
          )
        }
        .toDF()
        .join(finWideByVat.toDF(), Seq("vat"), "left_outer")
        .orderBy($"vat")

    // --- 4) Write CSV ---
    // Similar to other exports: Utils.saveAsCsvFile handles delimiter/header in your project.
    Utils.saveAsCsvFile(
      exportDF,
      "Organizations_NACEBEL56_2019_2023_NoEstablishments.csv"
    )

    nace56OrgsDS.unpersist()
    fin2019_2023.unpersist()
  }

  // --- Output helper case classes (typed transformations) ---

  private case class ExportOrgBase(
    vat: String,
    name: String,
    address: String,
    yearFounded: Option[Int]
  )

  private case class AnnualRow(
    vat: String,
    year: Int,
    financialHealthIndicator: Option[Double],
    equity: Option[Double],
    grossMargin: Option[Double],
    profit: Option[Double],
    ebitda: Option[Double],
    employees: Option[Int]
  )

  private case class FinancialsWide(
    vat: String,

    fhi2019: Option[Double],
    equity2019: Option[Double],
    grossMargin2019: Option[Double],
    profit2019: Option[Double],
    ebitda2019: Option[Double],
    employees2019: Option[Int],

    fhi2020: Option[Double],
    equity2020: Option[Double],
    grossMargin2020: Option[Double],
    profit2020: Option[Double],
    ebitda2020: Option[Double],
    employees2020: Option[Int],

    fhi2021: Option[Double],
    equity2021: Option[Double],
    grossMargin2021: Option[Double],
    profit2021: Option[Double],
    ebitda2021: Option[Double],
    employees2021: Option[Int],

    fhi2022: Option[Double],
    equity2022: Option[Double],
    grossMargin2022: Option[Double],
    profit2022: Option[Double],
    ebitda2022: Option[Double],
    employees2022: Option[Int],

    fhi2023: Option[Double],
    equity2023: Option[Double],
    grossMargin2023: Option[Double],
    profit2023: Option[Double],
    ebitda2023: Option[Double],
    employees2023: Option[Int]
  )
}
