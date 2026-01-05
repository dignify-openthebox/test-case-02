package be.openthebox.modules.adhoc

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import be.openthebox.dto._

// CUSTOMIZE: replace DTO imports with your actual project DTO packages/types if they differ
// e.g. OrganisationBO / VAT_AnnualAccount / EstablishmentBO / etc.

object ExportVatListEnrichmentToCsv extends App {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Export VAT List Enrichment (5y financials)")
    .getOrCreate()

  import spark.implicits._

  // ---------------------------
  // CUSTOMIZE: input / output paths
  // ---------------------------
  val vatListCsvPath        = args.lift(0).getOrElse("file:/data/input/vat_list.csv")
  val organizationsParquet  = args.lift(1).getOrElse("file:/data/parquet/organizations")
  val annualAccountsParquet = args.lift(2).getOrElse("file:/data/parquet/annual_accounts")
  val establishmentsParquet = args.lift(3).getOrElse("file:/data/parquet/establishments")
  val outputPath            = args.lift(4).getOrElse("file:/data/output/vat_enrichment_csv")

  // ---------------------------
  // Load VAT list (±25k)
  // ---------------------------
  val vatListDF = spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(vatListCsvPath)
    .select(trim(upper(col("vat"))).as("vat"))
    .filter(col("vat").isNotNull && length(col("vat")) > 0)
    .distinct()

  // ---------------------------
  // Load core datasets
  // ---------------------------
  val orgDS: Dataset[OrganisationBO] =
    spark.read.parquet(organizationsParquet).as[OrganisationBO]

  val aaDS: Dataset[VAT_AnnualAccount] =
    spark.read.parquet(annualAccountsParquet).as[VAT_AnnualAccount]

  // Establishment dataset type name may differ in your repo.
  // CUSTOMIZE: change EstablishmentBO to your establishment DTO.
  val estDS: Dataset[EstablishmentBO] =
    spark.read.parquet(establishmentsParquet).as[EstablishmentBO]

  // Broadcast VAT list because it is relatively small (25k)
  val orgFiltered = orgDS
    .join(broadcast(vatListDF), Seq("vat"), "inner")
    .cache()

  // ---------------------------
  // Establishments aggregation
  // ---------------------------
  val estAggDF = estDS
    .groupBy($"vat")
    .agg(
      count(lit(1)).as("establishmentCount")
      // CUSTOMIZE: if you need establishment details, add collect_list(struct(...))
    )

  // ---------------------------
  // Annual accounts: build last-5-years metrics per VAT
  // ---------------------------
  // We keep arrays of structs: [{year, value}, ...] to preserve year alignment.
  // Profit: using profitAfterTax (can be changed to operating profit / profit before tax).
  val aa5yDF = aaDS
    .select($"vat", explode_outer($"annualAccountSummaries").as("aas"))
    .select(
      $"vat",
      year($"aas.periodEndDate").as("year"),
      $"aas.profitLoss.turnoverO".as("turnover"),
      $"aas.profitLoss.profitAfterTaxO".as("profit"),
      $"aas.profitLoss.ebitdaO".as("ebitda"),
      $"aas.balanceSheet.equityO.currentO".as("equity"),
      // Code 22 (Land & Buildings) -> in sample financial JSON it's balanceSheet.landBuildingsO.currentO
      $"aas.balanceSheet.landBuildingsO.currentO".as("code22_land_buildings"),
      $"aas.numberOfEmployees".as("employeesFromAA")
    )
    .filter($"year".isNotNull)
    // Keep only recent 5 years per VAT (based on year)
    .withColumn(
      "rn",
      row_number().over(
        org.apache.spark.sql.expressions.Window
          .partitionBy($"vat")
          .orderBy($"year".desc)
      )
    )
    .filter($"rn" <= 5)
    .drop("rn")
    .groupBy($"vat")
    .agg(
      sort_array(collect_list(struct($"year", $"turnover"))).as("turnover5y"),
      sort_array(collect_list(struct($"year", $"profit"))).as("profit5y"),
      sort_array(collect_list(struct($"year", $"ebitda"))).as("ebitda5y"),
      sort_array(collect_list(struct($"year", $"equity"))).as("equity5y"),
      sort_array(collect_list(struct($"year", $"code22_land_buildings"))).as("code22LandBuildings5y"),
      sort_array(collect_list(struct($"year", $"employeesFromAA"))).as("employees5y")
    )

  // ---------------------------
  // Flatten organization fields needed
  // ---------------------------
  val orgFlatDF = orgFiltered
    .select(
      $"vat",
      // Names
      $"nameNlO".as("nameNl"),
      $"nameFrO".as("nameFr"),

      // Founding date
      $"foundingDateO".as("foundingDate"),

      // Health score (field name may differ; customize)
      $"healthScoreO".as("healthScore"),

      // Legal status (field names may differ; customize)
      $"legalStatusO".as("legalStatus"),

      // NACEBEL: choose primary if possible, else first
      // CUSTOMIZE if your Activity DTO differs
      expr("""
        filter(activities, a -> a.classification = 'NACEBEL' AND (a.typeLabel = 'Primary' OR a.typeLabel = 'PRIMARY'))
      """).as("primaryNaceActivities"),
      expr("""
        filter(activities, a -> a.classification = 'NACEBEL')
      """).as("allNaceActivities"),

      // Current address: pick addressHist with endDate null if exists
      $"addressHists".as("addressHists"),

      // Official publications link(s) - typically derived, not stored.
      // We'll fill via UDF hook below.
      lit(null).cast("string").as("officialPublicationLink")
    )
    .withColumn(
      "nacebelCode",
      coalesce(
        expr("element_at(primaryNaceActivities, 1).code"),
        expr("element_at(allNaceActivities, 1).code")
      )
    )
    .drop("primaryNaceActivities", "allNaceActivities")
    .withColumn(
      "currentAddress",
      expr("""
        element_at(
          transform(
            filter(addressHists, ah -> ah.endDate IS NULL),
            ah -> coalesce(ah.addressNlO, ah.addressFrO)
          ),
          1
        )
      """)
    )
    .withColumn("street", $"currentAddress.streetO")
    .withColumn("houseNumber", $"currentAddress.houseNumberO")
    .withColumn("box", $"currentAddress.boxO")
    .withColumn("postalCode", $"currentAddress.postalCodeO")
    .withColumn("city", $"currentAddress.cityO")
    .withColumn("countryCode", $"currentAddress.countryCode")
    .drop("addressHists", "currentAddress")

  // ---------------------------
  // CUSTOMIZE: official publication link builder
  // ---------------------------
  val publicationLinkUdf = udf { vat: String =>
    // Replace with the correct link format for your environment (Moniteur belge / NBB ref, etc.)
    // Example placeholder:
    if (vat == null || vat.isEmpty) "" else s"https://example.local/publications?vat=$vat"
  }

  val orgWithLinksDF = orgFlatDF.withColumn("officialPublicationLink", publicationLinkUdf($"vat"))

  // ---------------------------
  // Final join and output
  // ---------------------------
  val finalDF = orgWithLinksDF
    .join(aa5yDF, Seq("vat"), "left")
    .join(estAggDF, Seq("vat"), "left")
    // Employees: prefer org-level if you have it, otherwise keep AA 5y
    // CUSTOMIZE: if OrganisationBO has employees field, expose it above and coalesce here
    .withColumn("employeesCurrent", lit(null).cast("long"))

  finalDF
    .select(
      $"vat",
      $"nameNl",
      $"nameFr",
      $"foundingDate",
      $"healthScore",
      $"legalStatus",
      $"street", $"houseNumber", $"box", $"postalCode", $"city", $"countryCode",
      $"nacebelCode",
      $"establishmentCount",
      $"employeesCurrent",
      $"employees5y",
      $"turnover5y",
      $"profit5y",
      $"ebitda5y",
      $"equity5y",
      $"code22LandBuildings5y",
      $"officialPublicationLink"
    )
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", ";")
    .option("encoding", "UTF-8")
    .option("nullValue", "")
    .csv(outputPath)

  orgFiltered.unpersist()
}
