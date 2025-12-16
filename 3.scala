package be.openthebox.modules.adhoc

import be.openthebox.Utils
import be.openthebox.aggregates.{CalcOrganizationAggregates, CalcPersonAggregates}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ExportIranCompaniesAndPersonsToCsv extends App {

  implicit val spark: SparkSession =
    SparkSession.builder()
      .appName("Export Iran Companies and Persons")
      .getOrCreate()

  import spark.implicits._

  // =========================
  // CUSTOMIZE: target country
  // =========================
  val targetCountryCode = "IR"

  // =========================
  // Helpers
  // =========================
  private def createFormattedAddress(addressHist: be.openthebox.model.AddressHist): Option[String] = {
    val address = addressHist.addressNlO.orElse(addressHist.addressFrO)
    address.map { ad =>
      val elements = List(
        Option(ad.number),
        Option(ad.street),
        Option(ad.zipCode),
        Option(ad.city),
        ad.provinceO,
        Option(ad.countryCode)
      ).flatten.map(_.toString)

      elements.mkString(",")
    }
  }

  // =========================
  // Load aggregates
  // =========================
  val personDS = CalcPersonAggregates.loadResultsFromParquet
  val organizationDS = CalcOrganizationAggregates.loadResultsFromParquet

  // =========================
  // Persons related to Iran
  // =========================
  val iranPersonsDF =
    personDS
      .filter { p =>
        p.addressHists.exists { ah =>
          ah.addressNlO.orElse(ah.addressFrO).exists(a => a.countryCode != null && a.countryCode.equalsIgnoreCase(targetCountryCode))
        }
      }
      .map { p =>
        (
          p.pid,
          p.firstName,
          p.lastName,
          p.addressHists.lastOption.flatMap(createFormattedAddress)
        )
      }
      .toDF("pid", "firstName", "lastName", "lastAddress")
      .distinct()

  Utils.saveAsCsvFile(iranPersonsDF, s"IranPersons.csv")

  // =========================
  // Companies related to Iran
  // =========================
  val iranCompaniesDF =
    organizationDS
      .filter { org =>
        org.addressHists.exists { ah =>
          ah.addressNlO.orElse(ah.addressFrO).exists(a => a.countryCode != null && a.countryCode.equalsIgnoreCase(targetCountryCode))
        }
      }
      .map { org =>
        (
          org.vat,
          org.nameNl,
          org.nameFr,
          org.addressHists.lastOption.flatMap(createFormattedAddress)
        )
      }
      .toDF("vat", "nameNl", "nameFr", "lastAddressForCompany")
      .distinct()

  Utils.saveAsCsvFile(iranCompaniesDF, s"IranCompanies.csv")

  // =========================
  // Iran persons with company data (administrators/shareholders)
  // =========================
  // CUSTOMIZE: categories to include (ADMINISTER only vs ADMINISTER + SHAREHOLDER)
  val mandateCategoriesToInclude = Seq("ADMINISTER") // add "SHAREHOLDER" if desired

  val orgMandatesExploded =
    organizationDS
      .filter(org => org.personMandates.exists(pm => mandateCategoriesToInclude.contains(pm.category)))
      .map { c =>
        (
          c.vat,
          c.nameNl,
          c.nameFr,
          c.addressHists.lastOption.flatMap(createFormattedAddress),
          c.personMandates
        )
      }
      .toDF("vat", "nameNl", "nameFr", "lastAddressForCompany", "personMandates")
      .select($"vat", $"nameNl", $"nameFr", $"lastAddressForCompany", explode($"personMandates").as("personMandates"))
      .select(
        $"vat",
        $"nameNl",
        $"nameFr",
        $"lastAddressForCompany",
        $"personMandates.pid".as("pid"),
        $"personMandates.category".as("mandateCategory"),
        $"personMandates.startDateO".as("mandateStartDate"),
        $"personMandates.endDateO".as("mandateEndDate")
      )

  val iranAdministratorsWithCompanyDataDF =
    iranPersonsDF
      .join(orgMandatesExploded, Seq("pid"), "inner")
      .select(
        $"pid",
        $"firstName",
        $"lastName",
        $"lastAddress",
        $"vat",
        $"nameNl",
        $"nameFr",
        $"lastAddressForCompany",
        $"mandateCategory",
        $"mandateStartDate",
        $"mandateEndDate"
      )
      .distinct()

  Utils.saveAsCsvFile(iranAdministratorsWithCompanyDataDF, s"IranAdministratorsWithCompanyData.csv")

  spark.stop()
}
