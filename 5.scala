package be.openthebox.modules.adhoc

import be.openthebox.dto._
import be.openthebox.modules.Module
import be.openthebox.modules.InputArgs
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.LocalDate

/**
 * Colruyt-like export enriched with:
 * - 5-year financial history (turnover, profit, EBITDA, equity, land & buildings (code 22))
 * - founding date, financial health score
 * - address, legal status
 * - establishments
 * - number of employees
 * - NACEBEL codes
 * - official publication links
 *
 * CUSTOMIZE:
 * - input VAT list path
 * - parquet input locations (org, annual accounts, establishments, publications)
 * - output path + format (CSV/JSON)
 */
object ExportColruytLikePortfolioEnrichedToJson extends Module {

  // ========= Output schemas =========

  case class AnnualAccountSummaryForExport(
    periodEndDate: java.sql.Date,
    turnover: Option[Double],
    profit: Option[Double],
    ebitda: Option[Double],
    equity: Option[Double],
    landBuildings: Option[Double],            // Code 22 (land & buildings value)
    financialHealthIndicator: Option[Double]  // if available on annual accounts
  )

  case class EstablishmentForExport(
    nameNl: String,
    nameFr: String,
    startDate: Option[java.sql.Date],
    endDateO: Option[java.sql.Date],
    addressNlO: Option[Address],
    addressFrO: Option[Address],
    telephone: Option[String],
    email: Option[String],
    website: Option[String],
    activities: Array[String]
  )

  case class PublicationForExport(
    publicationDate: java.sql.Date,
    publicationNumber: String,
    pdfLink: Option[String],
    language: Option[String],
    subjects: Array[String],
    source: Option[String]
  )

  case class OrganizationForExport(
    vat: String,
    nameNl: String,
    nameFr: String,

    foundingDate: Option[java.sql.Date],
    legalStatus: Option[String],            // customize mapping if your model uses juridicalForm/situation
    financialHealthScore: Option[Double],   // org-level indicator
    numberOfEmployees: Option[Int],

    addressNlO: Option[Address],
    addressFrO: Option[Address],

    nacebelCodes: Array[String],

    establishments: Array[EstablishmentForExport],
    publications: Array[PublicationForExport],

    annualAccountSummaries5Y: Array[AnnualAccountSummaryForExport],

    exportMetadataCreatedAt: Timestamp
  )

  override def execute(inputArgs: InputArgs)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    // ========= Load inputs =========

    // CUSTOMIZE: VAT list location (Colruyt portfolio)
    val vatListPath = inputArgs.inputPath.getOrElse("s3://.../input/colruyt_vats.csv")

    // CUSTOMIZE: parquet locations in your repo/data lake
    val orgPath = inputArgs.getOrElse("orgPath", "s3://.../parquet/CalcOrganizationAggregates")
    val annualAccountsPath = inputArgs.getOrElse("annualAccountsPath", "s3://.../parquet/CalcVatsWithAnnualAccountSummary")
    val establishmentsPath = inputArgs.getOrElse("establishmentsPath", "s3://.../parquet/ImportEstablishments")
    val publicationsPath = inputArgs.getOrElse("publicationsPath", "s3://.../parquet/EnrichPublicationsWithEntities")

    val outputPath = inputArgs.outputPath.getOrElse(s"s3://.../output/colruyt_like_export_${System.currentTimeMillis()}")

    // VAT list as Dataset[String]
    val vatListDS: Dataset[String] =
      spark.read
        .option("header", "true")
        .option("delimiter", ";")
        .csv(vatListPath)
        .select(trim(col("vat")).as("vat"))
        .as[String]
        .filter(_.nonEmpty)
        .distinct()

    // Core datasets
    val orgDS: Dataset[OrganisationBO] =
      spark.read.parquet(orgPath).as[OrganisationBO]

    val annualDS: Dataset[VatWithAnnualAccountSummary] =
      spark.read.parquet(annualAccountsPath).as[VatWithAnnualAccountSummary]

    val estDS: Dataset[Establishment] =
      spark.read.parquet(establishmentsPath).as[Establishment]

    val pubDS: Dataset[Publication] =
      spark.read.parquet(publicationsPath).as[Publication]

    // ========= Restrict to portfolio =========

    val portfolioOrgs = orgDS.joinWith(vatListDS.toDF("vat"), orgDS("vat") === col("vat"), "inner")
      .map(_._1)
      .cache()

    // ========= Prepare enrichments =========

    // Establishments grouped per VAT
    val estByVat = estDS
      .groupByKey(_.vat)
      .mapGroups { case (vat, it) =>
        val arr = it.toArray
          .sortBy(_.startDate.map(_.getTime).getOrElse(Long.MinValue))
          .map(convertToEstablishmentForExport)
        (vat, arr)
      }.toDF("vat", "establishments")

    // Publications grouped per VAT (keep all or last N)
    // CUSTOMIZE: if Publication contains organizations[] rather than direct vat, adapt the mapping.
    val pubsByVat = pubDS
      .flatMap { p =>
        // This assumes publication.organizations contains VATs or identifiers.
        // CUSTOMIZE: adjust extraction to your Publication schema.
        val vats: Seq[String] =
          Option(p.organizations).toSeq.flatten
            .flatMap(o => Option(o.vat)) // if it’s an object with .vat
        vats.distinct.map(v => (v, p))
      }
      .groupByKey(_._1)
      .mapGroups { case (vat, it) =>
        val pubs = it.map(_._2).toArray
          .sortBy(_.publicationDate.getTime)
          .takeRight(50) // CUSTOMIZE: keep last N
          .map(convertToPublicationForExport)
        (vat, pubs)
      }.toDF("vat", "publications")

    // Annual accounts: convert to 5 most recent by periodEndDate
    val aaByVat = annualDS
      .map { v =>
        val last5 = convertToAnnualAccountSummaryForExport(v)
          .sortBy(_.periodEndDate.getTime)
          .takeRight(5)
        (v.vat, last5)
      }.toDF("vat", "annualAccountSummaries5Y")

    // ========= Final join & mapping =========

    val exportCreatedAt = new Timestamp(System.currentTimeMillis())

    val enriched =
      portfolioOrgs.toDF()
        .join(estByVat, Seq("vat"), "left")
        .join(pubsByVat, Seq("vat"), "left")
        .join(aaByVat, Seq("vat"), "left")
        .as[(OrganisationBO, Option[Array[EstablishmentForExport]], Option[Array[PublicationForExport]], Option[Array[AnnualAccountSummaryForExport]])](
          Encoders.tuple(
            Encoders.product[OrganisationBO],
            Encoders.kryo[Option[Array[EstablishmentForExport]]],
            Encoders.kryo[Option[Array[PublicationForExport]]],
            Encoders.kryo[Option[Array[AnnualAccountSummaryForExport]]]
          )
        )
        .map { case (org, estO, pubsO, aaO) =>
          OrganizationForExport(
            vat = org.vat,
            nameNl = org.nameNl,
            nameFr = org.nameFr,

            foundingDate = org.startDateO, // founding date
            legalStatus = Option(org.juridicalForm).orElse(Option(org.juridicalSituation)), // CUSTOMIZE field choice
            financialHealthScore = org.financialHealthIndicatorO,
            numberOfEmployees = org.employeeCountO,

            addressNlO = org.addressNlO,
            addressFrO = org.addressFrO,

            // NACEBEL codes: the Colruyt export uses NaceActivityUtils.extractNaceActivities(org.activities)
            // Here we keep a simple extraction (CUSTOMIZE if you have a helper util)
            nacebelCodes = extractNacebelCodes(org.activities),

            establishments = estO.getOrElse(Array.empty),
            publications = pubsO.getOrElse(Array.empty),

            annualAccountSummaries5Y = aaO.getOrElse(Array.empty),

            exportMetadataCreatedAt = exportCreatedAt
          )
        }

    // ========= Write output =========
    // JSON is usually better for nested arrays (establishments, publications, annual accounts).
    enriched
      .coalesce(1)
      .write
      .mode("overwrite")
      .json(outputPath)

    // If you truly need CSV: you must flatten arrays first (not included here).
  }

  // ========= Converters / helpers =========

  private def convertToAnnualAccountSummaryForExport(v: VatWithAnnualAccountSummary): Array[AnnualAccountSummaryForExport] = {
    Option(v.annualAccountSummaries).toSeq.flatten.map { aas =>
      AnnualAccountSummaryForExport(
        periodEndDate = aas.periodEndDate,
        profit = aas.balanceSheet.gainLossPeriodO.flatMap(_.currentO), // matches KB snippet
        turnover = aas.balanceSheet.turnoverO.flatMap(_.currentO),
        ebitda = aas.financialAnalysisO.flatMap(_.ebitda),
        equity = aas.balanceSheet.equityO.flatMap(_.currentO),
        landBuildings = aas.balanceSheet.landBuildingsO.flatMap(_.currentO), // Code 22
        financialHealthIndicator = aas.financialAnalysisO.flatMap(_.financialHealthIndicator)
      )
    }.toArray
  }

  private def convertToEstablishmentForExport(e: Establishment): EstablishmentForExport =
    EstablishmentForExport(
      nameNl = e.nameNl,
      nameFr = e.nameFr,
      startDate = e.startDate,
      endDateO = e.endDateO,
      addressNlO = e.addressNlO,
      addressFrO = e.addressFrO,
      telephone = e.telephone,
      email = e.email,
      website = e.website,
      activities = e.activities
    )

  private def convertToPublicationForExport(p: Publication): PublicationForExport =
    PublicationForExport(
      publicationDate = p.publicationDate,
      publicationNumber = p.publicationNumber,
      pdfLink = Option(p.pdfLink),
      language = Option(p.language),
      subjects = Option(p.subjects).getOrElse(Array.empty),
      source = Option(p.source)
    )

  private def extractNacebelCodes(activities: Array[String]): Array[String] = {
    // CUSTOMIZE: if you have structured NACE activities, use that.
    // Here we keep items that look like NACEBEL codes.
    Option(activities).getOrElse(Array.empty).filter { s =>
      val t = s.trim
      t.nonEmpty && t.exists(_.isDigit) && t.length <= 10
    }
  }
}
