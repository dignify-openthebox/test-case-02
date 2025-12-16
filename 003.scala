package be.openthebox.modules.adhoc

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import be.openthebox.dto._

import java.sql.Date

object ExportVatRepresentativesToCsv extends App {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Export VAT (20) + Representatives + NACEBEL + Address")
    .getOrCreate()

  import spark.implicits._

  // ---------------------------
  // CUSTOMIZE: input / output paths
  // ---------------------------
  val organizationsPath   = sys.env.getOrElse("ORGANIZATIONS_PARQUET", "s3://bucket/organizations.parquet")
  val personsPath         = sys.env.getOrElse("PERSONS_PARQUET", "s3://bucket/persons.parquet")
  val vatListPath         = sys.env.getOrElse("VAT_LIST", "s3://bucket/input/vats.csv")
  val outputPath          = sys.env.getOrElse("OUTPUT_PATH", "s3://bucket/output/vat_export_csv")

  // ---------------------------
  // Load datasets
  // ---------------------------
  val orgDS: Dataset[OrganisationBO] =
    spark.read.parquet(organizationsPath).as[OrganisationBO]

  val personDS: Dataset[PersonBO] =
    spark.read.parquet(personsPath).as[PersonBO]

  // VAT list: accept either header vat or single column
  val vatListDF = spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(vatListPath)

  val vatCol =
    if (vatListDF.columns.map(_.toLowerCase).contains("vat")) col(vatListDF.columns.find(_.toLowerCase == "vat").get)
    else col(vatListDF.columns.head)

  val vatList = vatListDF
    .select(trim(vatCol).as("vat"))
    .filter(length($"vat") > 0)
    .distinct()

  // Broadcast is fine for 20 VATs
  val orgFiltered = orgDS
    .join(broadcast(vatList), Seq("vat"), "inner")
    .cache()

  // ---------------------------
  // Helpers (null/Option safe)
  // ---------------------------
  def pickMainName(o: OrganisationBO): String =
    o.nameNlO.orElse(o.nameFrO).orElse(o.nameO).getOrElse("")

  def pickCurrentAddressHist(addressHists: Seq[AddressHist]): Option[AddressHist] =
    addressHists
      .filter(_.endDateO.isEmpty)
      .sortBy(_.startDateO.map(_.getTime).getOrElse(Long.MinValue))
      .lastOption
      .orElse(addressHists.sortBy(_.startDateO.map(_.getTime).getOrElse(Long.MinValue)).lastOption)

  def pickMostRecentPreviousAddressHist(addressHists: Seq[AddressHist]): Option[AddressHist] =
    addressHists
      .filter(_.endDateO.nonEmpty)
      .sortBy(_.endDateO.map(_.getTime).getOrElse(Long.MinValue))
      .lastOption

  def addressToLine(a: AddressBO): String =
    Seq(
      a.streetO.getOrElse(""),
      a.houseNumberO.getOrElse(""),
      a.boxO.getOrElse("")
    ).filter(_.nonEmpty).mkString(" ").trim

  def addressToPostcode(a: AddressBO): String =
    a.zipCodeO.getOrElse("")

  def addressToCountry(a: AddressBO): String =
    a.countryCode.getOrElse("")

  def addressToCity(a: AddressBO): String =
    a.municipalityO.getOrElse("")

  // ---------------------------
  // Flatten org + representative mandates
  // ---------------------------
  val orgMandatesDF = orgFiltered
    .toDF()
    .select(
      $"vat",
      $"nameNlO", $"nameFrO", $"nameO",
      $"addressHists",
      $"activities",
      explode_outer($"personMandates").as("mandate")
    )
    .select(
      $"vat",
      $"nameNlO", $"nameFrO", $"nameO",
      $"addressHists",
      $"activities",
      $"mandate.pid".as("pid"),
      $"mandate.role".as("mandateRole"),
      $"mandate.function".as("mandateFunction"),
      $"mandate.startDateO".as("mandateStartDate"),
      $"mandate.endDateO".as("mandateEndDate")
    )

  // Join persons to get first/last name + determine type
  val personsSlim = personDS
    .toDF()
    .select(
      $"pid",
      $"firstNameO",
      $"lastNameO",
      $"nameO",              // some datasets use name for moral persons
      $"isMoralPersonO",     // CUSTOMIZE if field differs in your PersonBO
    )

  val joined = orgMandatesDF
    .join(personsSlim, Seq("pid"), "left_outer")

  // ---------------------------
  // NACEBEL principal activity extraction
  // ---------------------------
  // We keep it in Scala mapping to be robust to Option/Seq shapes.
  val outputDS = joined.as[JoinedRow]
    .map { r =>
      val orgName = r.nameNlO.orElse(r.nameFrO).orElse(r.nameO).getOrElse("")

      val currentAddrHistO = pickCurrentAddressHist(r.addressHists.getOrElse(Seq.empty))
      val prevAddrHistO    = pickMostRecentPreviousAddressHist(r.addressHists.getOrElse(Seq.empty))

      val currentAddrO = currentAddrHistO.flatMap(ah => ah.addressNlO.orElse(ah.addressFrO))
      val prevAddrO    = prevAddrHistO.flatMap(ah => ah.addressNlO.orElse(ah.addressFrO))

      val addressLine     = currentAddrO.map(addressToLine).getOrElse("")
      val postcode        = currentAddrO.map(addressToPostcode).getOrElse("")
      val country         = currentAddrO.map(addressToCountry).getOrElse("")
      val additionalAddr  = prevAddrO.map(a => s"${addressToLine(a)}, ${addressToPostcode(a)} ${addressToCity(a)}, ${addressToCountry(a)}").getOrElse("")

      val principalNaceO = r.activities
        .getOrElse(Seq.empty)
        .find(a =>
          a.typeLabelO.exists(_.equalsIgnoreCase("Primary")) &&
            a.classificationO.exists(c => c.equalsIgnoreCase("NACEBEL_2008") || c.equalsIgnoreCase("NACEBEL"))
        )
        .orElse(
          r.activities.getOrElse(Seq.empty).find(a =>
            a.classificationO.exists(c => c.equalsIgnoreCase("NACEBEL_2008") || c.equalsIgnoreCase("NACEBEL"))
          )
        )

      val naceCode = principalNaceO.flatMap(_.codeO).getOrElse("")
      val naceDesc = principalNaceO.flatMap(_.descriptionO).getOrElse("")

      val repFirst = r.firstNameO.getOrElse("")
      val repLast  = r.lastNameO.orElse(r.personNameO).getOrElse("")

      val repType =
        r.isMoralPersonO match {
          case Some(true)  => "moral"
          case Some(false) => "physical"
          case None        => "" // CUSTOMIZE: decide default
        }

      val repStatus =
        if (r.mandateEndDate.isEmpty) "current" else "previous"

      OutputRow(
        vat = r.vat,
        name = orgName,
        address = addressLine,
        postcode = postcode,
        country = country,
        nacebel2008PrincipalCode = naceCode,
        nacebel2008PrincipalDescription = naceDesc,
        representativeFirstName = repFirst,
        representativeLastName = repLast,
        representativeType = repType,
        representativeStatus = repStatus,
        additionalAddress = additionalAddr
      )
    }

  // ---------------------------
  // Write CSV
  // ---------------------------
  outputDS
    .toDF()
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", ";")
    .option("encoding", "UTF-8")
    .option("nullValue", "")
    .csv(outputPath)

  spark.stop()

  // ---------------------------
  // Schemas for typed mapping
  // ---------------------------

  // Represents the joined row after mandate explode + person join (keep Option fields to be safe)
  case class JoinedRow(
    vat: String,
    nameNlO: Option[String],
    nameFrO: Option[String],
    nameO: Option[String],
    addressHists: Option[Seq[AddressHist]],
    activities: Option[Seq[ActivityBO]],
    pid: String,
    mandateRole: Option[String],
    mandateFunction: Option[String],
    mandateStartDate: Option[Date],
    mandateEndDate: Option[Date],
    firstNameO: Option[String],
    lastNameO: Option[String],
    personNameO: Option[String],
    isMoralPersonO: Option[Boolean]
  )

  case class OutputRow(
    vat: String,
    name: String,
    address: String,
    postcode: String,
    country: String,
    nacebel2008PrincipalCode: String,
    nacebel2008PrincipalDescription: String,
    representativeFirstName: String,
    representativeLastName: String,
    representativeType: String,           // moral/physical
    representativeStatus: String,         // current/previous
    additionalAddress: String
  )
}
