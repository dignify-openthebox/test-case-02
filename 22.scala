package be.openthebox.modules.adhoc

import be.openthebox.Utils
import be.openthebox.modules.CalcOrganizationAggregates
import be.openthebox.modules.CalcPersonAggregates
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ExportCountryRelatedCompaniesAndPersonsToCsv extends App {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Export Country Related Companies And Persons")
    .getOrCreate()

  import spark.implicits._

  // =========================
  // CUSTOMIZE: target country
  // =========================
  // Examples: "IR" (Iran), "CN" (China), "RU" (Russia)
  val targetCountryCode = sys.props.getOrElse("country", "CN").trim.toUpperCase

  // =========================
  // Load source datasets
  // =========================
  val organizationDS = CalcOrganizationAggregates.loadResultsFromParquet
  val personDS = CalcPersonAggregates.loadResultsFromParquet

  // =========================
  // Helpers
  // =========================
  private def normCc(cc: String): String = Option(cc).getOrElse("").trim.toUpperCase

  private def addressCountryCol(addressHistCol: org.apache.spark.sql.Column): org.apache.spark.sql.Column = {
    // addressHist has: addressNlO / addressFrO; take either and read .countryCode
    upper(coalesce(
      addressHistCol.getField("addressNlO").getField("countryCode"),
      addressHistCol.getField("addressFrO").getField("countryCode"),
      lit("")
    ))
  }

  private def formatAddressFromHist(addressHistCol: org.apache.spark.sql.Column): org.apache.spark.sql.Column = {
    val ad = coalesce(addressHistCol.getField("addressNlO"), addressHistCol.getField("addressFrO"))

    // mirrors the pattern in existing exports: number,street,zip,city,province,countryCode
    concat_ws(",",
      ad.getField("number").cast("string"),
      ad.getField("street").cast("string"),
      ad.getField("zipCode").cast("string"),
      ad.getField("city").cast("string"),
      ad.getField("provinceO").cast("string"),
      ad.getField("countryCode").cast("string")
    )
  }

  private def isBelgianOrg(orgAddressHistCol: org.apache.spark.sql.Column): org.apache.spark.sql.Column = {
    val cc = addressCountryCol(orgAddressHistCol)
    // In the China export example, Belgian = "BE" OR empty country code
    cc === lit("BE") || cc === lit("")
  }

  private def isTargetCountry(orgAddressHistCol: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    addressCountryCol(orgAddressHistCol) === lit(targetCountryCode)

  // Take the "last" known address (matches the exports that use addressHists.lastOption)
  // If addressHists is empty, result will be null.
  private def lastAddressHistCol(addressHistsCol: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    element_at(addressHistsCol, -1)

  // =========================
  // 1) Persons from target country
  // =========================
  val personsFromTargetCountry: DataFrame =
    personDS.toDF
      .withColumn("addressHist", explode_outer($"addressHists"))
      .withColumn("addrCountry", addressCountryCol($"addressHist"))
      .filter($"addrCountry" === targetCountryCode)
      .groupBy($"pid", $"firstName", $"lastName")
      .agg(
        // lastAddress = person's last addressHist formatted (same idea as existing exports)
        formatAddressFromHist(lastAddressHistCol($"addressHists")).as("lastAddress")
      )
      .select($"pid", $"firstName", $"lastName", $"lastAddress")
      .distinct()

  Utils.saveAsCsvFile(personsFromTargetCountry, s"${targetCountryCode}Persons.csv")

  // =========================
  // 2) Administrators/shareholders from target country with their companies
  // =========================
  val orgMandatesExploded: DataFrame =
    organizationDS.toDF
      .filter(size($"personMandates") > 0)
      .withColumn("lastAddressForCompany", formatAddressFromHist(lastAddressHistCol($"addressHists")))
      .select(
        $"vat",
        $"nameNl",
        $"nameFr",
        $"lastAddressForCompany",
        explode($"personMandates").as("mandate")
      )
      .filter(
        $"mandate.category" === lit("ADMINISTER") || $"mandate.category" === lit("SHAREHOLDER")
      )
      .select(
        $"vat",
        $"nameNl",
        $"nameFr",
        $"lastAddressForCompany",
        $"mandate.pid".as("pid")
      )

  val administratorsWithCompanyData: DataFrame =
    personsFromTargetCountry
      .join(orgMandatesExploded, Seq("pid"), "inner")
      .select(
        $"pid",
        $"firstName",
        $"lastName",
        $"lastAddress",
        $"vat",
        $"nameNl",
        $"nameFr",
        $"lastAddressForCompany"
      )

  Utils.saveAsCsvFile(administratorsWithCompanyData, s"${targetCountryCode}AdministratorsWithCompanyData.csv")

  // =========================
  // 3) Target-country companies owned by Belgian companies
  //    (Belgian company has participationHists containing a target-country company vat)
  // =========================
  val belgianCompaniesWithParticipations: DataFrame =
    organizationDS.toDF
      .filter(exists($"addressHists", ah => isBelgianOrg(ah)))
      .flatMap { row =>
        // Keep this as DataFrame ops; but we already are in DF. We'll do it with explode:
        Seq(row)
      }

  val beToTargetViaParticipation: DataFrame =
    organizationDS.toDF
      .filter(exists($"addressHists", ah => isBelgianOrg(ah)))
      .select(
        $"vat".as("vatBE"),
        $"nameNl".as("nameNlBE"),
        $"nameFr".as("nameFrBE"),
        explode_outer($"participationHists").as("part")
      )
      .select(
        $"vatBE",
        $"nameNlBE",
        $"nameFrBE",
        $"part.vat".as("vat"),
        $"part.interestsHeldType".as("type"),
        $"part.interestsHeldDirectlyPercentageO".as("percentage"),
        $"part.source".as("source")
      )

  val targetCountryCompaniesBasic: DataFrame =
    organizationDS.toDF
      .filter(exists($"addressHists", ah => isTargetCountry(ah)))
      .select(
        $"vat",
        $"nameNl",
        $"nameFr",
        formatAddressFromHist(lastAddressHistCol($"addressHists")).as("lastAddress")
      )

  val targetCountryCompaniesOwnedByBelgianCompanies: DataFrame =
    beToTargetViaParticipation
      .join(targetCountryCompaniesBasic, Seq("vat"), "inner")
      .select(
        $"vatBE", $"nameNlBE", $"nameFrBE",
        $"vat", $"nameNl", $"nameFr", $"lastAddress",
        $"type", $"percentage", $"source"
      )

  Utils.saveAsCsvFile(
    targetCountryCompaniesOwnedByBelgianCompanies,
    s"${targetCountryCode}CompaniesOwnedByBelgianCompanies.csv"
  )

  // =========================
  // 4) Belgian companies owned by target-country companies
  // =========================
  val targetToBeViaParticipation: DataFrame =
    organizationDS.toDF
      .filter(exists($"addressHists", ah => isTargetCountry(ah)))
      .select(
        $"vat".as("vat"),
        $"nameNl".as("nameNl"),
        $"nameFr".as("nameFr"),
        formatAddressFromHist(lastAddressHistCol($"addressHists")).as("lastAddress"),
        explode_outer($"participationHists").as("part")
      )
      .select(
        $"vat",
        $"nameNl",
        $"nameFr",
        $"lastAddress",
        $"part.vat".as("vatBE"),
        $"part.interestsHeldType".as("type"),
        $"part.interestsHeldDirectlyPercentageO".as("percentage"),
        $"part.source".as("source")
      )

  val belgianCompaniesBasic: DataFrame =
    organizationDS.toDF
      .filter(exists($"addressHists", ah => isBelgianOrg(ah)))
      .select(
        $"vat".as("vatBE"),
        $"nameNl".as("nameNlBE"),
        $"nameFr".as("nameFrBE")
      )

  val belgianCompaniesOwnedByTargetCountryCompanies: DataFrame =
    targetToBeViaParticipation
      .join(belgianCompaniesBasic, Seq("vatBE"), "inner")
      .select(
        $"vat", $"nameNl", $"nameFr", $"lastAddress",
        $"vatBE", $"nameNlBE", $"nameFrBE",
        $"type", $"percentage", $"source"
      )

  Utils.saveAsCsvFile(
    belgianCompaniesOwnedByTargetCountryCompanies,
    s"BelgianCompaniesOwnedBy${targetCountryCode}Companies.csv"
  )
}
