package be.openthebox.modules.adhoc

import org.apache.spark.sql.{Dataset, SparkSession}
import be.openthebox.dto._
import be.openthebox.modules.calc._
import org.apache.spark.sql.functions._

/**
 * Full dump export of core datasets:
 *  - Organizations (vat key)
 *  - Persons (pid key)
 *  - Annual account summaries (vat key)
 *
 * CUSTOMIZE:
 *  - outputBasePath
 *  - output format (parquet/csv/json)
 *  - optional filters (date range, only active, etc.)
 */
object ExportAllCoreDatasets extends App {

  implicit val spark: SparkSession =
    SparkSession.builder()
      .appName("Export All Core Datasets")
      .getOrCreate()

  import spark.implicits._

  // CUSTOMIZE: where to write
  val outputBasePath = args.headOption.getOrElse("s3://my-bucket/exports/full_dump")

  // Load datasets (project standard loaders)
  val organizationDS: Dataset[OrganisationBO] =
    CalcOrganizationAggregates.loadResultsFromParquet

  val personDS: Dataset[PersonBO] =
    CalcPersonAggregates.loadResultsFromParquet

  val vatWithAnnualAccountSummaryDS: Dataset[VatWithAnnualAccountSummary] =
    CalcVatsWithAnnualAccountSummary.loadResultsFromParquet

  // OPTIONAL: If you truly want "everything", keep as-is.
  // Example optional filters:
  // val orgs = organizationDS.filter(_.vat != null).filter(_.vat.nonEmpty)
  val orgs = organizationDS
  val persons = personDS
  val annuals = vatWithAnnualAccountSummaryDS

  // Performance: repartition by key for large writes
  val orgsOut = orgs.repartition(400, $"vat")
  val personsOut = persons.repartition(400, $"pid")
  val annualsOut = annuals.repartition(400, $"vat")

  // Write Parquet (recommended for full dumps)
  orgsOut.write.mode("overwrite").parquet(s"$outputBasePath/organizations_parquet")
  personsOut.write.mode("overwrite").parquet(s"$outputBasePath/persons_parquet")
  annualsOut.write.mode("overwrite").parquet(s"$outputBasePath/annual_accounts_parquet")

  // If you need CSV instead, prefer partitioned CSV (NOT single file) for big exports:
  // orgsOut
  //   .toDF()
  //   .write.mode("overwrite")
  //   .option("header", "true")
  //   .option("delimiter", ";")
  //   .option("encoding", "UTF-8")
  //   .option("nullValue", "")
  //   .csv(s"$outputBasePath/organizations_csv")
}
