package be.openthebox.modules.adhoc

import be.openthebox.modules.{CalcOrganizationAggregates, CalcPersonAggregates, Module}
import be.openthebox.model.{Address, AddressHist}
import be.openthebox.util.{InputArgs, Utils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object ExportVatListWithRepresentativesToCsv extends Module {

  override def execute(inputArgs: InputArgs)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    spark.sparkContext.setJobGroup(this.getClass.getName, this.getClass.getName)
    spark.sparkContext.setJobDescription("Export 20 VAT numbers with reps + NACEBEL principal")

    // ----------------------------
    // CUSTOMIZE: provide VAT list (20 vats)
    // Tip: keep the BE prefix formatting consistent with your stored vat format.
    // ----------------------------
    val vats: Seq[String] = Seq(
      "BE0412435684"
      // ... 19 more
    )

    // ----------------------------
    // Helpers
    // ----------------------------
    def pickAddress(addressHist: AddressHist): Option[Address] =
      addressHist.addressNlO.orElse(addressHist.addressFrO)

    def formatAddress(addressO: Option[Address]): String =
      addressO.map { a =>
        // street + number + zip + city (+ country)
        val parts = List(
          Option(a.street).filter(_.nonEmpty),
          Option(a.number).filter(_.nonEmpty),
          Option(a.zipCode).filter(_.nonEmpty),
          Option(a.city).filter(_.nonEmpty),
          Option(a.countryCode).filter(_.nonEmpty)
        ).flatten
        parts.mkString(", ")
      }.getOrElse("")

    def extractZip(addressO: Option[Address]): String =
      addressO.flatMap(a => Option(a.zipCode)).getOrElse("")

    def extractCountry(addressO: Option[Address]): String =
      addressO.flatMap(a => Option(a.countryCode)).getOrElse("")

    // Prefer current address (endDate empty), else last known
    def currentOrLastAddressHist(addressHists: Seq[AddressHist]): Option[AddressHist] = {
      val current = addressHists.find(_.endDate.isEmpty)
      current.orElse(addressHists.lastOption)
    }

    // ----------------------------
    // Load base datasets
    // ----------------------------
    val orgDS = CalcOrganizationAggregates
      .loadResultsFromParquet
      .filter(o => vats.contains(o.vat))
      .cache()

    val personDS = CalcPersonAggregates
      .loadResultsFromParquet
      // small optimization: only keep people that could appear in mandates of these companies
      // (still safe if you remove this)
      .cache()

    // ----------------------------
    // Organization base fields + current/last address + principal NACEBEL 2008
    // ----------------------------
    val orgBaseDF = orgDS.map { o =>
      val addrHistO = currentOrLastAddressHist(o.addressHists)
      val addrO = addrHistO.flatMap(pickAddress)

      // CUSTOMIZE: principal NACE extraction depends on your activities model.
      // We assume something like: activities with classification "NACEBEL_2008" and typeLabel "Primary".
      val principalNaceO = o.activities
        .find(a =>
          Option(a.classification).exists(_.toUpperCase.contains("NACE")) &&
          Option(a.typeLabel).exists(_.toUpperCase.contains("PRIMARY"))
        )

      val naceCode = principalNaceO.flatMap(a => Option(a.code)).getOrElse("")
      val naceDesc = principalNaceO.flatMap(a => Option(a.descriptionNl).orElse(Option(a.descriptionFr))).getOrElse("")

      (
        o.vat,
        Option(o.nameNl).filter(_.nonEmpty).getOrElse(Option(o.nameFr).getOrElse("")),
        formatAddress(addrO),
        extractZip(addrO),
        extractCountry(addrO),
        naceCode,
        naceDesc,
        // Additional address: if you mean "second address line", adapt here.
        // If you mean "previous company address", we provide the latest non-current.
        formatAddress(
          o.addressHists.filter(_.endDate.nonEmpty).lastOption.flatMap(h => pickAddress(h))
        ),
        o.personMandates // explode later
      )
    }.toDF(
      "vat",
      "name",
      "address",
      "postcode",
      "country",
      "nace_principal_code",
      "nace_principal_description",
      "additional_address",
      "personMandates"
    )

    // ----------------------------
    // Explode mandates (representatives)
    // ----------------------------
    val mandatesDF = orgBaseDF
      .select(
        $"vat",
        $"name",
        $"address",
        $"postcode",
        $"country",
        $"nace_principal_code",
        $"nace_principal_description",
        $"additional_address",
        explode_outer($"personMandates").as("mandate")
      )
      .select(
        $"vat",
        $"name",
        $"address",
        $"postcode",
        $"country",
        $"nace_principal_code",
        $"nace_principal_description",
        $"additional_address",

        // Representative identifiers
        $"mandate.pid".as("rep_pid"),

        // CUSTOMIZE: if mandates can also be organizations (moral person),
        // you might have fields like mandate.organizationVat or mandate.entityType, etc.
        // We'll derive "type" later; keep raw fields if available.
        $"mandate.category".as("rep_category"),
        $"mandate.startDate".as("rep_start_date"),
        $"mandate.endDate".as("rep_end_date")
      )
      .withColumn(
        "rep_status",
        when($"rep_end_date".isNull, lit("CURRENT")).otherwise(lit("PREVIOUS"))
      )

    // ----------------------------
    // Join representative names (physical persons)
    // If you also need moral persons (organization mandates), we can add a second join to orgDS by rep VAT.
    // ----------------------------
    val personsSlimDF = personDS
      .select($"pid", $"firstName".as("rep_first_name"), $"lastName".as("rep_last_name"))

    val resultDF = mandatesDF
      .join(personsSlimDF, mandatesDF("rep_pid") === personsSlimDF("pid"), "left")
      .drop(personsSlimDF("pid"))
      .withColumn(
        // CUSTOMIZE: If you have a reliable way to detect moral vs physical:
        // - If rep_pid is non-null => physical person
        // - Else if you have orgVat on mandate => moral person
        "rep_type",
        when($"rep_pid".isNotNull, lit("physical")).otherwise(lit("moral"))
      )
      .select(
        $"vat".as("vat_id"),
        $"name",
        $"address",
        $"postcode",
        $"country",
        $"nace_principal_code",
        $"nace_principal_description",
        $"rep_first_name",
        $"rep_last_name",
        $"rep_type",
        $"rep_status",            // CURRENT / PREVIOUS
        $"additional_address",
        $"rep_category",
        $"rep_start_date",
        $"rep_end_date"
      )

    // ----------------------------
    // Write CSV
    // ----------------------------
    Utils.saveAsCsvFile(
      resultDF.coalesce(1),
      "VatList_With_Representatives.csv"
    )

    orgDS.unpersist()
    personDS.unpersist()
  }
}
