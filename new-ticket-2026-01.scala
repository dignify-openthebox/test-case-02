package be.openthebox.modules.adhoc

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Date

// CUSTOMIZE: adjust imports to your project DTOs
import be.openthebox.dto._
// OrganisationBO, Establishment, VatWithAnnualAccountSummary, AnnualAccountSummary, Address, etc.

object ExportVatPortfolioEnrichedToCsv extends App {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Export VAT Portfolio Enriched")
    .getOrCreate()

  import spark.implicits._

  // =========================
  // CUSTOMIZE: input paths
  // =========================
  val vatListPath          = args.lift(0).getOrElse("input/vats.csv")               // one VAT per line or CSV with header
  val organisationsPath    = args.lift(1).getOrElse("parquet/organisations")
  val annualAccountsPath   = args.lift(2).getOrElse("parquet/annualAccounts")
  val establishmentsPathO  = args.lift(3) // optional (if you have establishments in separate parquet)
  val outputPath           = args.lift(4).getOrElse("output/vat_portfolio_export")

  // =========================
  // Load VAT list (~25k)
  // =========================
  val vatListDF =
    spark.read
      .option("header", "true")
      .option("sep", ";")
      .csv(vatListPath)
      .select(trim(col("vat")).as("vat"))
      .filter(col("vat").isNotNull && length(col("vat")) > 0)
      .dropDuplicates("vat")

  // =========================
  // Load core datasets
  // =========================
  val orgDS: Dataset[OrganisationBO] =
    spark.read.parquet(organisationsPath).as[OrganisationBO]

  val aaDS: Dataset[VatWithAnnualAccountSummary] =
    spark.read.parquet(annualAccountsPath).as[VatWithAnnualAccountSummary]

  // Optional establishments dataset (if separate)
  val estDSO: Option[Dataset[Establishment]] =
    establishmentsPathO.map(p => spark.read.parquet(p).as[Establishment])

  // =========================
  // Filter orgs by VAT list (broadcast join)
  // =========================
  val filteredOrgDF =
    orgDS.toDF
      .join(broadcast(vatListDF), Seq("vat"), "inner")
      .cache()

  // =========================
  // Annual accounts: compute last 5 years snapshot per VAT
  // We follow the same field mapping as in ExportColruytToDataFile.scala:
  // profit, turnover, ebitda, equity, landBuildings, financialHealthIndicator
  // =========================

  case class AnnualAccountSummaryForExport(
    periodEndDate: Date,
    turnover: Option[Double],
    profit: Option[Double],
    ebitda: Option[Double],
    equity: Option[Double],
    landBuildings: Option[Double],
    financialHealthIndicator: Option[Double]
  )

  private def last5Summaries(v: VatWithAnnualAccountSummary): Seq[AnnualAccountSummaryForExport] = {
    val sorted = v.annualAccountSummaries
      .sortBy(_.periodEndDate.getTime)

    sorted.takeRight(5).map { s =>
      // IMPORTANT: field locations can differ by schema version in some datasets.
      // This matches the snippet you provided from ExportColruytToDataFile.scala.
      AnnualAccountSummaryForExport(
        periodEndDate = s.periodEndDate,
        profit = s.balanceSheet.gainLossPeriodO.flatMap(_.currentO),
        turnover = s.balanceSheet.turnoverO.flatMap(_.currentO),
        ebitda = s.financialAnalysisO.flatMap(_.ebitda),
        equity = s.balanceSheet.equityO.flatMap(_.currentO),
        landBuildings = s.balanceSheet.landBuildingsO.flatMap(_.currentO), // Code 22: Land & Buildings
        financialHealthIndicator = s.financialAnalysisO.flatMap(_.financialHealthIndicator)
      )
    }
  }

  val aaLast5DF =
    aaDS
      .toDF
      .join(broadcast(vatListDF), Seq("vat"), "inner")
      .as[VatWithAnnualAccountSummary]
      .map { v =>
        val last5 = last5Summaries(v)
        val latestFhi = last5.sortBy(_.periodEndDate.getTime).lastOption.flatMap(_.financialHealthIndicator)
        (v.vat, last5, latestFhi)
      }
      .toDF("vat", "annualAccountsLast5", "healthScoreFromAAO") // if you use org.healthScore instead, keep both
      .cache()

  // =========================
  // Establishments aggregation (count + optionally JSON)
  // =========================
  val estAggDF =
    estDSO match {
      case Some(estDS) =>
        estDS.toDF
          .join(broadcast(vatListDF), Seq("vat"), "inner")
          .groupBy(col("vat"))
          .agg(
            count(lit(1)).as("establishmentsCount"),
            // keep a small nested payload if needed
            // CUSTOMIZE: if too large, remove collect_list.
            collect_list(
              struct(
                col("establishmentNumber"),
                col("nameNl"), col("nameFr"),
                col("startDate"), col("endDateO"),
                col("addressNlO"), col("addressFrO"),
                col("telephone"), col("email"), col("website"),
                col("activities")
              )
            ).as("establishments")
          )
      case None =>
        spark.emptyDataFrame
          .select(lit(null).cast("string").as("vat"))
          .limit(0)
    }

  // =========================
  // Helpers: pick current address, NACEBEL, publication links
  // =========================
  // CUSTOMIZE: adapt to your OrganisationBO structure (field names may differ)
  def currentAddressExpr(prefix: String): Column = {
    // tries NL then FR; takes "current" address hist (endDate is null), else latest
    // This is kept in Column form to avoid UDFs.
    val addrHist = col(s"$prefix.addressHists")
    val current = expr(s"filter($prefix.addressHists, x -> x.endDate is null)")
    val picked = when(size(current) > 0, element_at(current, -1)).otherwise(element_at(addrHist, -1))
    // prefer addressNlO over addressFrO
    coalesce(picked.getField("addressNlO"), picked.getField("addressFrO"))
  }

  val orgEnrichedDF =
    filteredOrgDF
      .withColumn("currentAddress", currentAddressExpr(""))
      .withColumn("name", coalesce(col("nameNlO"), col("nameFrO")))
      // NACEBEL: pick primary (CUSTOMIZE: adapt fields; common pattern is activities[] with classification/typeLabel)
      .withColumn(
        "nacebelPrimary",
        expr("filter(activities, x -> x.classification = 'NACEBEL' AND x.typeLabel = 'Primary')")[0]
      )
      .withColumn("nacebelCode", col("nacebelPrimary.code"))
      // Official publication links
      // CUSTOMIZE: replace with your fields; common is org.officialPublications / kboLinks.
      .withColumn("officialPublicationLinks", col("officialPublicationLinksO"))
      // Founding date, legal status, employees, health score
      // CUSTOMIZE: update field names if your DTO differs
      .withColumnRenamed("foundingDateO", "foundingDate")
      .withColumnRenamed("legalStatusO", "legalStatus")
      .withColumnRenamed("numberOfEmployeesO", "employees")
      .withColumnRenamed("healthScoreO", "healthScoreOrgO")

  // =========================
  // Final join
  // =========================
  val joined =
    orgEnrichedDF
      .join(aaLast5DF, Seq("vat"), "left")
      .transform { df =>
        if (estDSO.isDefined) df.join(estAggDF, Seq("vat"), "left") else df
      }
      // Prefer health score: organization health score if present else annual account indicator
      .withColumn("healthScore",
        coalesce(col("healthScoreOrgO"), col("healthScoreFromAAO"))
      )

  // =========================
  // Flatten 5-year metrics into columns
  // =========================
  // annualAccountsLast5: array of structs(periodEndDate, turnover, profit, ebitda, equity, landBuildings, financialHealthIndicator)
  // We create y1..y5 ordered by periodEndDate.
  val withSorted5 =
    joined.withColumn(
      "aa5",
      expr("transform(array_sort(annualAccountsLast5, (l, r) -> case when l.periodEndDate < r.periodEndDate then -1 when l.periodEndDate > r.periodEndDate then 1 else 0 end), x -> x)")
    )

  def aaCol(idxFromEnd: Int, field: String): Column =
    expr(s"element_at(aa5, size(aa5) - ${idxFromEnd - 1}).$field")

  val finalDF =
    withSorted5
      .select(
        col("vat"),
        col("name"),
        col("foundingDate"),
        col("legalStatus"),
        col("healthScore"),
        col("currentAddress").as("address"),
        col("nacebelCode"),
        // Employees (if from org); CUSTOMIZE if you want employees from latest AA instead
        col("employees"),
        // Establishments (count)
        (if (estDSO.isDefined) col("establishmentsCount") else lit(null).cast("long").as("establishmentsCount")),
        // Official publication links
        col("officialPublicationLinks"),

        // 5-year columns: oldest..latest (y1..y5)
        aaCol(5, "periodEndDate").as("y1_periodEndDate"),
        aaCol(5, "turnover").as("y1_turnover"),
        aaCol(5, "profit").as("y1_profit"),
        aaCol(5, "ebitda").as("y1_ebitda"),
        aaCol(5, "equity").as("y1_equity"),
        aaCol(5, "landBuildings").as("y1_landBuildings"), // Code 22

        aaCol(4, "periodEndDate").as("y2_periodEndDate"),
        aaCol(4, "turnover").as("y2_turnover"),
        aaCol(4, "profit").as("y2_profit"),
        aaCol(4, "ebitda").as("y2_ebitda"),
        aaCol(4, "equity").as("y2_equity"),
        aaCol(4, "landBuildings").as("y2_landBuildings"),

        aaCol(3, "periodEndDate").as("y3_periodEndDate"),
        aaCol(3, "turnover").as("y3_turnover"),
        aaCol(3, "profit").as("y3_profit"),
        aaCol(3, "ebitda").as("y3_ebitda"),
        aaCol(3, "equity").as("y3_equity"),
        aaCol(3, "landBuildings").as("y3_landBuildings"),

        aaCol(2, "periodEndDate").as("y4_periodEndDate"),
        aaCol(2, "turnover").as("y4_turnover"),
        aaCol(2, "profit").as("y4_profit"),
        aaCol(2, "ebitda").as("y4_ebitda"),
        aaCol(2, "equity").as("y4_equity"),
        aaCol(2, "landBuildings").as("y4_landBuildings"),

        aaCol(1, "periodEndDate").as("y5_periodEndDate"),
        aaCol(1, "turnover").as("y5_turnover"),
        aaCol(1, "profit").as("y5_profit"),
        aaCol(1, "ebitda").as("y5_ebitda"),
        aaCol(1, "equity").as("y5_equity"),
        aaCol(1, "landBuildings").as("y5_landBuildings")
      )

  // =========================
  // Write CSV
  // =========================
  finalDF
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", ";")
    .option("encoding", "UTF-8")
    .option("nullValue", "")
    .csv(outputPath)
}
