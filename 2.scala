package be.openthebox.modules.adhoc

import be.openthebox.model._
import be.openthebox.util._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Country-based cross-border analysis export (Iran).
 *
 * Outputs (CSV):
 *  - IranPersons.csv
 *  - IranAdministratorsWithCompanyData.csv
 *  - IranCompaniesOwnedByBelgianCompanies.csv
 *  - BelgianCompaniesOwnedByIranCompanies.csv
 *
 * CUSTOMIZE:
 *  - input parquet locations (if not using Calc* loaders)
 *  - exact mandate role string for administrators (ADMINISTER vs ADMINISTRATOR, etc.)
 *  - relationship fields (participationHists / affiliationHists) depending on your Organization model
 */
object ExportIranCountryExportToCsv extends App {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("Export Iran Country Export To CSV")
    .getOrCreate()

  import spark.implicits._

  // --------------------------
  // Config
  // --------------------------
  val targetCountryCode = "IR"
  val belgiumCountryCodes = Set("BE", "") // per requirements catalog: BE or empty treated as Belgium

  // Output folder (will contain 4 subfolders, one per CSV)
  val outBase = sys.props.getOrElse("out", "exports/country-based/iran")

  // --------------------------
  // Load datasets
  // --------------------------
  // Preferred in this project (per docs):
  val organizationDS: Dataset[Organization] = CalcOrganizationAggregates.loadResultsFromParquet
  val personDS: Dataset[Person] = CalcPersonAggregates.loadResultsFromParquet

  // --------------------------
  // Helpers
  // --------------------------
  private def normCountry(cc: String): String = Option(cc).getOrElse("").trim.toUpperCase

  private def addressCountryCodeFromHist(ah: AddressHist): Option[String] = {
    // Uses NL/FR address variants; picks whichever exists
    ah.addressNlO
      .orElse(ah.addressFrO)
      .flatMap(a => Option(a.countryCode).map(normCountry))
      .filter(_.nonEmpty)
  }

  private def lastAddressAsString(addressHists: Seq[AddressHist]): String = {
    // Choose "latest" by endDate/startDate heuristic (endDate empty = current)
    val sorted = addressHists.sortBy { ah =>
      val end = ah.endDateO.map(_.getTime).getOrElse(Long.MaxValue)
      val start = ah.startDateO.map(_.getTime).getOrElse(0L)
      (end, start)
    }
    val lastO = sorted.lastOption.flatMap(ah => ah.addressNlO.orElse(ah.addressFrO))
    lastO.map { a =>
      Seq(a.street, a.houseNumber, a.zipCode, a.municipality, a.box, a.countryCode)
        .flatMap(x => Option(x).map(_.trim).filter(_.nonEmpty))
        .mkString(",")
    }.getOrElse("")
  }

  private def orgLastAddressAsString(org: Organization): String =
    lastAddressAsString(org.addressHists)

  private def personHasCountry(person: Person, cc: String): Boolean =
    person.addressHists.exists(ah => addressCountryCodeFromHist(ah).contains(normCountry(cc)))

  private def orgHasCountry(org: Organization, cc: String): Boolean =
    org.addressHists.exists(ah => addressCountryCodeFromHist(ah).contains(normCountry(cc)))

  private def orgIsBelgian(org: Organization): Boolean = {
    // requirement catalog: BE or empty
    org.addressHists.exists { ah =>
      addressCountryCodeFromHist(ah).exists(c => belgiumCountryCodes.contains(c))
    }
  }

  // --------------------------
  // Output 1: Persons from Iran
  // --------------------------
  case class IranPersonRow(pid: String, firstName: String, lastName: String, lastAddress: String)

  val iranPersons: Dataset[IranPersonRow] =
    personDS
      .filter(p => personHasCountry(p, targetCountryCode))
      .map { p =>
        IranPersonRow(
          pid = p.pid,
          firstName = p.firstName.getOrElse(""),
          lastName = p.lastName.getOrElse(""),
          lastAddress = lastAddressAsString(p.addressHists)
        )
      }

  // --------------------------
  // Output 2: Iranian administrators with Belgian company data
  // --------------------------
  // CUSTOMIZE: confirm the exact admin role label in person mandates
  val adminRoleLabels = Set("ADMINISTER", "ADMINISTRATOR")

  case class IranAdminWithCompanyRow(
    pid: String,
    firstName: String,
    lastName: String,
    lastAddress: String,
    vat: String,
    nameNl: String,
    nameFr: String,
    lastAddressForCompany: String
  )

  val orgMandatesExploded =
    organizationDS
      .select(
        $"vat",
        $"nameNl",
        $"nameFr",
        $"addressHists",
        explode_outer($"personMandates").as("mandate")
      )
      .select(
        $"vat",
        $"nameNl",
        $"nameFr",
        $"addressHists",
        $"mandate.pid".as("pid"),
        $"mandate.role".as("role")
      )

  val iranAdminsWithCompanyData =
    iranPersons
      .toDF
      .join(orgMandatesExploded, Seq("pid"), "inner")
      .filter(upper(trim($"role")).isin(adminRoleLabels.toSeq: _*))
      .join(organizationDS.select($"vat".as("vat2")), $"vat" === $"vat2", "left") // keeps schema stable
      .drop("vat2")
      .as[(IranPersonRow, String, String, String, Seq[AddressHist], String, String)] // not used; just schema hint
      // map via Row-DF approach for simplicity in unknown model versions:
      .toDF("pid", "firstName", "lastName", "lastAddress", "vat", "nameNl", "nameFr", "addressHists", "role")
      .as[(String, String, String, String, String, String, String, Seq[AddressHist], String)]
      .map { case (pid, fn, ln, pAddr, vat, nameNl, nameFr, addrHists, _) =>
        IranAdminWithCompanyRow(
          pid = pid,
          firstName = fn,
          lastName = ln,
          lastAddress = pAddr,
          vat = vat,
          nameNl = nameNl,
          nameFr = nameFr,
          lastAddressForCompany = lastAddressAsString(addrHists)
        )
      }
      // keep only Belgian companies (per catalog intent: Iranian persons administering Belgian companies)
      .joinWith(organizationDS.select($"vat".as("ov"), $"addressHists".as("oh")).as[(String, Seq[AddressHist])],
        $"vat" === $"ov",
        "inner"
      )
      .filter { case (_, (ov, oh)) =>
        // reuse heuristic on address hists; treat BE/empty as Belgian
        oh.exists(ah => addressCountryCodeFromHist(ah).exists(c => belgiumCountryCodes.contains(c)))
      }
      .map(_._1)

  // --------------------------
  // Output 3 & 4: Ownership cross-border
  // --------------------------
  // NOTE: The exact field names can differ across versions.
  // This implementation assumes Organization has participationHists with:
  //  - counterpartyVat / relatedVat-like field (owner/owned)
  //  - countryCode (of counterparty)
  //
  // CUSTOMIZE HERE if your model uses affiliationHists instead of participationHists.

  case class OwnershipRow(
    ownerVat: String,
    ownerName: String,
    ownerCountry: String,
    ownedVat: String,
    ownedName: String,
    ownedCountry: String
  )

  // Build a minimal org lookup
  val orgLookup = organizationDS
    .select(
      $"vat",
      coalesce($"nameNl", lit("")).as("nameNl"),
      coalesce($"nameFr", lit("")).as("nameFr"),
      $"addressHists"
    )
    .withColumn(
      "country",
      // take first non-empty country in history (fallback empty)
      expr("filter(transform(addressHists, x -> upper(trim(coalesce(x.addressNlO.countryCode, x.addressFrO.countryCode, '')))), x -> x <> '')[0]")
    )
    .select($"vat", $"nameNl", $"nameFr", $"country")
    .cache()

  // Explode participations
  val parts = organizationDS
    .select($"vat".as("orgVat"), explode_outer($"participationHists").as("p"))
    .select(
      $"orgVat",
      $"p.relatedVat".as("relatedVat"),      // CUSTOMIZE: field name may be different
      upper(trim($"p.countryCode")).as("relatedCountry") // CUSTOMIZE: may be stored elsewhere
    )

  // Iran companies owned by Belgian companies:
  // owner (BE) -> owned (IR)
  val iranCompaniesOwnedByBelgianCompanies =
    parts
      .join(orgLookup.withColumnRenamed("vat", "ownerVat"), $"orgVat" === $"ownerVat", "inner")
      .join(orgLookup.withColumnRenamed("vat", "ownedVat"), $"relatedVat" === $"ownedVat", "inner")
      .select(
        $"ownerVat",
        coalesce($"nameNl", $"nameFr").as("ownerName"),
        $"country".as("ownerCountry"),
        $"ownedVat",
        coalesce(col("nameNl").as("ownedNameNl"), col("nameFr").as("ownedNameFr")).as("ownedNameTmp") // workaround
      )

  // Because of potential name column clashes, do a clean select with aliases
  val owners = orgLookup.select($"vat".as("ownerVat"), coalesce($"nameNl", $"nameFr").as("ownerName"), $"country".as("ownerCountry"))
  val owneds = orgLookup.select($"vat".as("ownedVat"), coalesce($"nameNl", $"nameFr").as("ownedName"), $"country".as("ownedCountry"))

  val ownedByBelgian =
    parts
      .join(owners, $"orgVat" === $"ownerVat", "inner")
      .join(owneds, $"relatedVat" === $"ownedVat", "inner")
      .filter($"ownerCountry".isin(belgiumCountryCodes.toSeq: _*))
      .filter($"ownedCountry" === targetCountryCode)
      .select($"ownerVat", $"ownerName", $"ownerCountry", $"ownedVat", $"ownedName", $"ownedCountry")
      .as[OwnershipRow]

  // Belgian companies owned by Iran companies:
  // owner (IR) -> owned (BE)
  val belgianOwnedByIran =
    parts
      .join(owners, $"orgVat" === $"ownerVat", "inner")
      .join(owneds, $"relatedVat" === $"ownedVat", "inner")
      .filter($"ownerCountry" === targetCountryCode)
      .filter($"ownedCountry".isin(belgiumCountryCodes.toSeq: _*))
      .select($"ownerVat", $"ownerName", $"ownerCountry", $"ownedVat", $"ownedName", $"ownedCountry")
      .as[OwnershipRow]

  // --------------------------
  // Write CSVs
  // --------------------------
  def writeCsv[T](ds: Dataset[T], path: String): Unit =
    ds.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ";")
      .option("encoding", "UTF-8")
      .option("nullValue", "")
      .csv(path)

  writeCsv(iranPersons, s"$outBase/IranPersons.csv")
  writeCsv(iranAdminsWithCompanyData, s"$outBase/IranAdministratorsWithCompanyData.csv")
  writeCsv(ownedByBelgian, s"$outBase/IranCompaniesOwnedByBelgianCompanies.csv")
  writeCsv(belgianOwnedByIran, s"$outBase/BelgianCompaniesOwnedByIranCompanies.csv")

  spark.stop()
}
