/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.core.metadata.FlintMetadata

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.flint.datatype.FlintDataType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
 * Flint index interface in Spark.
 */
trait FlintSparkIndex extends Logging {

  /**
   * Index type
   */
  val kind: String

  /**
   * Index options
   */
  val options: FlintSparkIndexOptions

  /**
   * @return
   *   Flint index name
   */
  def name(): String

  /**
   * @return
   *   Flint index metadata
   */
  def metadata(): FlintMetadata

  /**
   * Build a data frame to represent index data computation logic. Upper level code decides how to
   * use this, ex. batch or streaming, fully or incremental refresh.
   *
   * @param spark
   *   Spark session for implementation class to use as needed
   * @param df
   *   data frame to append building logic. If none, implementation class create source data frame
   *   on its own
   * @return
   *   index building data frame
   */
  def build(spark: SparkSession, df: Option[DataFrame]): DataFrame
}

object FlintSparkIndex extends Logging {

  /**
   * Interface indicates a Flint index has custom streaming refresh capability other than foreach
   * batch streaming.
   */
  trait StreamingRefresh {

    /**
     * Build streaming refresh data frame.
     *
     * @param spark
     *   Spark session
     * @return
     *   data frame represents streaming logic
     */
    def buildStream(spark: SparkSession): DataFrame
  }

  /**
   * ID column name.
   */
  val ID_COLUMN: String = "__id__"

  /**
   * Generate an ID column in the precedence below: (1) Use ID expression directly if provided in
   * index option; (2) SHA-1 based on all aggregated columns if found; (3) SHA-1 based on source
   * file path and timestamp column; 4) No ID column generated
   *
   * @param df
   *   data frame to generate ID column for
   * @param idExpr
   *   ID expression option
   * @return
   *   optional ID column expression
   */
  def generateIdColumn(df: DataFrame, idExpr: Option[String]): Option[Column] = {
    def timestampColumn: Option[String] = {
      df.columns.toSet.intersect(Set("timestamp", "@timestamp")).headOption
    }

    if (idExpr.isDefined) {
      Some(expr(idExpr.get))
    } else if (timestampColumn.isDefined) {
      Some(sha1(concat(input_file_name(), col(timestampColumn.get))))
    } else {
      None
    }
  }

  /**
   * Common prefix of Flint index name which is "flint_database_table_"
   *
   * @param fullTableName
   *   source full table name
   * @return
   *   Flint index name
   */
  def flintIndexNamePrefix(fullTableName: String): String =
    s"flint_${fullTableName.replace(".", "_")}_"

  /**
   * Populate environment variables to persist in Flint metadata.
   *
   * @return
   *   env key value mapping to populate
   */
  def populateEnvToMetadata: Map[String, String] = {
    // TODO: avoid hardcoding env name below by providing another config
    val envNames = Seq("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "SERVERLESS_EMR_JOB_ID")
    envNames
      .flatMap(key =>
        Option(System.getenv(key))
          .map(value => key -> value))
      .toMap
  }

  /**
   * Create Flint metadata builder with common fields.
   *
   * @param index
   *   Flint index
   * @return
   *   Flint metadata builder
   */
  def metadataBuilder(index: FlintSparkIndex): FlintMetadata.Builder = {
    val builder = new FlintMetadata.Builder()
    // Common fields
    builder.kind(index.kind)
    builder.options(index.options.optionsWithDefault.mapValues(_.asInstanceOf[AnyRef]).asJava)

    // Index properties
    val envs = populateEnvToMetadata
    if (envs.nonEmpty) {
      builder.addProperty("env", envs.asJava)
    }

    // Optional index settings
    val settings = index.options.indexSettings()
    if (settings.isDefined) {
      builder.indexSettings(settings.get)
    }
    builder
  }

  def generateSchemaJSON(allFieldTypes: Map[String, String]): String = {
    // Backtick column names to escape special characters, otherwise fromDDL() will fail
    val catalogDDL =
      allFieldTypes
        .map { case (colName, colType) => s"`$colName` $colType not null" }
        .mkString(",")

    val structType = StructType.fromDDL(catalogDDL)
    FlintDataType.serialize(structType)
  }
}
