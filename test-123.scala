package be.openthebox.modules.adhoc

import be.openthebox.model
import be.openthebox.model.{AddressHist, Establishment}
import be.openthebox.modules._
import be.openthebox.util._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.LocalDate
import java.time.ZoneId
import java.util.Date

/**
 * VAT list enrichment export.
 *
 * Input:  CSV with one VAT per line (header optional)
 * Output: JSON datafile (1 file) containing enriched organisations
 *
 * Based on patterns used in ExportColruytToDataFile.scala:
 * - VAT list join
 * - annual accounts summaries mapping (turnover, profit, ebitda, equity, landBuildings)
 * - establishments mapping
 * - publications mapping (pdfLink)
 */
object ExportVatPortfolioEnrichedToDataFile extends Module {

  override def execute(inputArgs: InputArgs)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    spark.sparkContext.setJobGroup(this.getClass.getName, this.getClass.getName)
    spark.sparkContext.setJobDescription("Export VAT portfolio enriched datafile")

    // ----------------------------
    // CUSTOMIZE: input / output
    // ----------------------------
    val vatListPath =
      inputArgs.getOrElse("vatListPath", "s3://bucket/path/vat_list.csv")
    val outputPath =
      inputArgs.getOrElse("outputPath", "s3://bucket/path/output/vat_portfolio_datafile")

    // If you want a fixed "as-of" year, set it via args. Otherwise: current year.
    val asOfYear: Int =
      inputArgs.getOrElse("asOfYear", LocalDate.now().getYear.toString).toInt
    val yearFrom: Int = asOfYear - 4 // 5-year window

    // ----------------------------
    // Load portfolio VATs
    // ----------------------------
    val vatListDF = spark.read
      .option("header", "true")      // ok even if file has no header, we handle below
      .option("sep", ";")            // CUSTOMIZE: delimiter
      .option("inferSchema", "false")
      .csv(vatListPath)

    // Handle both "vat" header and "single column without header"
    val vatCol =
      if (vatListDF.columns.map(_.toLowerCase).contains("vat")) col("vat") else col(vatListDF.columns.head)

    val portfolioVatsDF = vatListDF
      .select(trim(regexp_replace(vatCol, "[^A-Za-z0-9]", "")).as("vat"))
      .filter(length($"vat") > 0)
      .distinct()
      .cache()

    logger.info(s"Portfolio VAT count: ${portfolioVatsDF.count()}")

    // Broadcast the VAT list (25k is safe) for faster joins
    val portfolioVatsBroadcast = broadcast(portfolioVatsDF)

    // ----------------------------
    // Load source datasets
    // ----------------------------
    val orgDS = CalcOrganizationAggregates.loadResultsFromParquet.cache()
    val vatsWithAA = CalcVatsWithAnnualAccountSummary.loadResultsFromParquet.cache()
    val establishmentDS = ImportEstablishments.loadResultsFromParquet.cache()

    // Publications / articles are used in Colruyt export for official links.
    // If your project uses different module names, adjust here.
    val publicationsDS = EnrichPublicationsWithEntities.loadResultsFromParquet
    val articlesDS = EnrichArticlesWithInsights.loadResultsFromParquet

    // ----------------------------
    // Filter organizations by portfolio VATs
    // ----------------------------
    val portfolioOrgsDS: Dataset[OrganisationBO] =
      orgDS.join(portfolioVatsBroadcast, Seq("vat"), "inner").as[OrganisationBO].cache()

    // ----------------------------
    // Establishments (group by vat)
    // ----------------------------
    val establishmentsByVatDF =
      establishmentDS
        .join(portfolioVatsBroadcast, Seq("vat"), "inner")
        .groupBy($"vat")
        .agg(collect_list(struct(
          $"nameNl",
          $"nameFr",
          $"startDate",
          $"addressNlO",
          $"addressFrO",
          $"telephone",
          $"email",
          $"website",
          $"activities",
          $"endDateO"
        )).as("establishments"))

    // ----------------------------
    // Annual accounts: keep only last 5 years, then collect per vat
    // We map out: turnover, profit, ebitda, equity, employees, landBuildings (code 22)
    // ----------------------------
    val aa5yByVatDF =
      vatsWithAA
        .join(portfolioVatsBroadcast, Seq("vat"), "inner")
        .select($"vat", explode($"annualAccountSummaries").as("aas"))
        .withColumn("year", year(to_date($"aas.periodEndDate")))
        .filter($"year".between(yearFrom, asOfYear))
        .select(
          $"vat",
          $"aas.periodEndDate".as("periodEndDate"),
          // IMPORTANT: use the same mapping pattern seen in ExportColruytToDataFile.scala snippet
          $"aas.balanceSheet.gainLossPeriodO.currentO".as("profit"),
          $"aas.balanceSheet.turnoverO.currentO".as("turnover"),
          $"aas.financialAnalysisO.ebitda".as("ebitda"),
          $"aas.balanceSheet.equityO.currentO".as("equity"),
          $"aas.balanceSheet.landBuildingsO.currentO".as("landBuildings"), // code 22 land & buildings
          $"aas.numberOfEmployees".as("employees"),
          $"aas.financialAnalysisO.financialHealthIndicator".as("financialHealthIndicatorAA")
        )
        .groupBy($"vat")
        .agg(
          sort_array(collect_list(struct(
            $"periodEndDate",
            $"turnover",
            $"profit",
            $"ebitda",
            $"equity",
            $"landBuildings",
            $"employees",
            $"financialHealthIndicatorAA"
          ))).as("annualAccounts5y")
        )

    // ----------------------------
    // Publications / official links (pdfLink); keep recent N
    // Articles are optional; you asked "Official publication links" so publications are primary.
    // ----------------------------
    val publicationsByVatDF =
      publicationsDS
        .join(portfolioVatsBroadcast, Seq("vat"), "inner")
        .groupBy($"vat")
        .agg(
          sort_array(collect_list(struct(
            $"publicationDate",
            $"publicationNumber",
            $"pdfLink",
            $"language",
            $"subjects",
            $"source"
          ))).as("publications")
        )

    val articlesByVatDF =
      articlesDS
        .join(portfolioVatsBroadcast, Seq("vat"), "inner")
        .groupBy($"vat")
        .agg(
          sort_array(collect_list(struct(
            $"publicationDate",
            $"title",
            $"paths"
          ))).as("articles")
        )

    // ----------------------------
    // Final join + output shaping
    // ----------------------------
    def currentAddressFromOrg(org: OrganisationBO): Option[model.Address] =
      org.addressHists
        .sortBy(_.startDateO.map(_.getTime).getOrElse(Long.MinValue))
        .lastOption
        .flatMap(ah => ah.addressNlO.orElse(ah.addressFrO))

    def primaryNaceFromOrg(org: OrganisationBO): Option[String] = {
      // CUSTOMIZE: if your project has explicit "primary" label, adjust extraction.
      // This is a safe fallback: first NACEBEL code.
      org.activities
        .find(a => Option(a.classification).exists(_.equalsIgnoreCase("NACEBEL")))
        .map(_.code)
        .orElse(org.activities.headOption.map(_.code))
    }

    val baseDF = portfolioOrgsDS.map { org =>
      EnrichedOrgBase(
        vat = org.vat,
        nameNl = org.nameNl,
        nameFr = org.nameFr,
        foundingDate = org.startDateO,
        healthScore = org.financialHealthIndicatorO, // "health score" in org aggregates
        employeeCount = org.employeeCountO,
        legalStatusForm = Option(org.juridicalForm).filter(_.nonEmpty),
        legalStatusSituation = Option(org.juridicalSituation).filter(_.nonEmpty),
        address = currentAddressFromOrg(org),
        nacebelCode = primaryNaceFromOrg(org)
      )
    }.toDF()

    val enrichedDF =
      baseDF
        .join(aa5yByVatDF, Seq("vat"), "left")
        .join(establishmentsByVatDF, Seq("vat"), "left")
        .join(publicationsByVatDF, Seq("vat"), "left")
        .join(articlesByVatDF, Seq("vat"), "left")
        // Keep only last 5 pubs/articles if huge
        .withColumn("publications", expr("slice(publications, -5, 5)"))
        .withColumn("articles", expr("slice(articles, -5, 5)"))
        .withColumn("exportedAt", lit(new Timestamp(System.currentTimeMillis())))

    // ----------------------------
    // Write output
    // ----------------------------
    // JSON datafile (recommended for nested arrays like establishments + 5y financials)
    enrichedDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("compression", "gzip") // CUSTOMIZE
      .json(outputPath)

    logger.info(s"Export written to: $outputPath")
  }

  // Small base row before joining nested datasets
  case class EnrichedOrgBase(
    vat: String,
    nameNl: String,
    nameFr: String,
    foundingDate: Option[Date],
    healthScore: Option[Double],
    employeeCount: Option[Int],
    legalStatusForm: Option[String],
    legalStatusSituation: Option[String],
    address: Option[model.Address],
    nacebelCode: Option[String]
  )
}
