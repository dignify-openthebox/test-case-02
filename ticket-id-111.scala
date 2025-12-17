package be.openthebox.modules.adhoc

import be.openthebox.model
import be.openthebox.model.AddressHist
import be.openthebox.modules.{CalcOrganizationAggregates, CalcVatsWithAnnualAccountSummary, Module}
import be.openthebox.util._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date
import scala.util.Try

object ExportOrganizationsNacebel56_2019_2023_ToCsv extends Module {

  // CUSTOMIZE: output filename
  private val outputFileName = "Organizations_NACEBEL56_2019_2023.csv"

  // CUSTOMIZE: year range
  private val fromYear = 2019
  private val toYear = 2023

  override def execute(inputArgs: InputArgs)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession.sparkContext.setJobGroup(this.getClass.getName, this.getClass.getName)
    sparkSession.sparkContext.setJobDescription("Export organizations with NACEBEL 56 and financials 2019-2023")

    def createFormattedAddress(addressHist: AddressHist): Option[String] = {
      val address: Option[model.Address] = addressHist.addressNlO.orElse(addressHist.addressFrO)
      address.map { ad =>
        val elements = List(
          Option(ad.number).filter(_.nonEmpty),
          Option(ad.street).filter(_.nonEmpty),
          Option(ad.zipCode).filter(_.nonEmpty),
          Option(ad.city).filter(_.nonEmpty),
          ad.provinceO.filter(_.nonEmpty),
          Option(ad.countryCode).filter(_.nonEmpty)
        ).flatten

        elements.mkString(",")
      }
    }

    def foundedYearFromStartDate(startDateO: Option[Date]): Option[Int] =
      startDateO.flatMap(d => Try(d.toLocalDate.getYear).toOption)

    // Loads only legal entities (organizations). Establishments are a separate dataset (ImportEstablishments),
    // so by not using it we effectively "exclude establishments".
    val organizationDS = CalcOrganizationAggregates.loadResultsFromParquet

    val annualAccountDS = CalcVatsWithAnnualAccountSummary.loadResultsFromParquet

    // 1) Filter to organizations with NACEBEL "56" (56, 56.10, 56.2, ...)
    // Also enforce classification if present in your Activity model. If not available, keep startsWith only.
    val nace56OrgsDS = organizationDS.filter { org =>
      org.activities.exists { act =>
        val codeOk = Option(act.code).exists(_.startsWith("56"))
        // CUSTOMIZE: if your Activity has classification field, you can enforce it:
        // val classOk = Option(act.classification).forall(_.equalsIgnoreCase("NACEBEL"))
        // codeOk && classOk
        codeOk
      }
    }

    // 2) Flatten annual account summaries for 2019–2023 into (vat, year, metrics...)
    // NOTE: Field paths based on knowledge-base docs:
    // - equity: annualAccountSummary.balanceSheet.equityO.flatMap(_.currentO)
    // - gross margin: annualAccountSummary.profitLoss.grossMarginO
    // - profit: prefer profitAfterTaxO, fallback profitBeforeTaxO, fallback operatingProfitO
    // - ebitda: annualAccountSummary.profitLoss.ebitdaO (or financialAnalysisO.ebitda in some schemas)
    // - employees: annualAccountSummary.numberOfEmployees
    //
    // If your exact model differs, adjust the extractors below.
    val financialsFlatDF =
      annualAccountDS
        .flatMap { vatWithSummary =>
          vatWithSummary.annualAccountSummaries.toSeq.flatMap { aas =>
            val year = Try(aas.periodEndDate.toLocalDate.getYear).toOption
            year
              .filter(y => y >= fromYear && y <= toYear)
              .map { y =>
                val equityO = aas.balanceSheet.equityO.flatMap(_.currentO)

                val grossMarginO = aas.profitLoss.grossMarginO

                val profitO =
                  aas.profitLoss.profitAfterTaxO
                    .orElse(aas.profitLoss.profitBeforeTaxO)
                    .orElse(aas.profitLoss.operatingProfitO)

                val ebitdaO =
                  aas.profitLoss.ebitdaO
                    .orElse(aas.financialAnalysisO.flatMap(_.ebitda)) // fallback for other schema variants

                val employeesO = aas.numberOfEmployees

                val finHealthO = aas.financialAnalysisO.flatMap(_.financialHealthIndicator)

                FinancialYearRow(
                  vat = vatWithSummary.vat,
                  year = y,
                  financialHealthIndicator = finHealthO,
                  equity = equityO,
                  grossMargin = grossMarginO,
                  profit = profitO,
                  ebitda = ebitdaO,
                  employees = employeesO
                )
              }
          }
        }
        .toDF

    // 3) Prepare organization base fields
    val orgBaseDF =
      nace56OrgsDS
        .map { org =>
          val name =
            Option(org.nameNl).filter(_.nonEmpty)
              .orElse(Option(org.nameFr).filter(_.nonEmpty))
              .getOrElse("")

          val address =
            org.addressHists.lastOption.flatMap(createFormattedAddress).getOrElse("")

          val foundedYear = foundedYearFromStartDate(org.startDateO)

          // Some org aggregates already have a financialHealthIndicatorO on org level;
          // but requirement is for 2019–2023. We'll take per-year from annual accounts.
          // Still, we keep founded year + org metadata here.
          OrgBaseRow(
            vat = org.vat,
            name = name,
            address = address,
            foundedYear = foundedYear
          )
        }
        .toDF

    // 4) Join base org data with yearly financials => one row per company per year
    val exportDF =
      orgBaseDF
        .join(financialsFlatDF, Seq("vat"), "left_outer")
        .select(
          $"vat",
          $"name",
          $"address",
          $"foundedYear",
          $"year",
          $"financialHealthIndicator",
          $"equity",
          $"grossMargin",
          $"profit",
          $"ebitda",
          $"employees"
        )
        // optional: keep only rows where we actually have a year in range
        .filter($"year".isNotNull)
        .orderBy($"vat", $"year")

    logger.info(s"Saving $outputFileName")
    Utils.saveAsCsvFile(exportDF, outputFileName)
  }

  // Output row helpers (used for typed transformations before DF)
  private case class OrgBaseRow(
    vat: String,
    name: String,
    address: String,
    foundedYear: Option[Int]
  )

  private case class FinancialYearRow(
    vat: String,
    year: Int,
    financialHealthIndicator: Option[Double],
    equity: Option[Double],
    grossMargin: Option[Double],
    profit: Option[Double],
    ebitda: Option[Double],
    employees: Option[Int]
  )
}
