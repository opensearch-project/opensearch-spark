/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.metadata.FlintJsonHelper._

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Project}
import org.apache.spark.sql.flint.datatype.FlintDataType
import org.apache.spark.sql.functions.{col, concat_ws, expr, sha1, to_json}
import org.apache.spark.sql.types.{MapType, StructType}

/**
 * Flint index interface in Spark.
 */
trait FlintSparkIndex {

  /**
   * Index type
   */
  val kind: String

  /**
   * Index options
   */
  val options: FlintSparkIndexOptions

  /**
   * Latest metadata log entry for index
   */
  val latestLogEntry: Option[FlintMetadataLogEntry]

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

object FlintSparkIndex {

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
   * Common prefix of Flint index name which is "flint_database_table_"
   *
   * @param fullTableName
   *   source full table name
   * @return
   *   Flint index name
   */
  def flintIndexNamePrefix(fullTableName: String): String = {
    require(fullTableName.split('.').length >= 3, s"Table name $fullTableName is not qualified")

    // Keep all parts since the third as it is
    val parts = fullTableName.split('.')
    s"flint_${parts(0)}_${parts(1)}_${parts.drop(2).mkString(".")}"
  }

  /**
   * Add backticks to table name to escape special character
   *
   * @param fullTableName
   *   source full table name
   * @return
   *   quoted table name
   */
  def quotedTableName(fullTableName: String): String = {
    require(fullTableName.split('.').length >= 3, s"Table name $fullTableName is not qualified")

    val parts = fullTableName.split('.')
    s"${parts(0)}.${parts(1)}.`${parts.drop(2).mkString(".")}`"
  }

  /**
   * Generate an ID column in the precedence below:
   * ```
   * 1. Use ID expression provided in the index option;
   * 2. SHA-1 based on all output columns if aggregated;
   * 3. Otherwise, no ID column generated.
   * ```
   *
   * @param df
   *   which DataFrame to generate ID column
   * @param options
   *   Flint index options
   * @return
   *   DataFrame with/without ID column
   */
  def generateIdColumn(df: DataFrame, options: FlintSparkIndexOptions): DataFrame = {
    // Assume output rows must be unique if a simple query plan has aggregate operator
    def isAggregated: Boolean =
      df.queryExecution.logical.exists(_.isInstanceOf[Aggregate])

    options.idExpression() match {
      case Some(idExpr) if idExpr.nonEmpty =>
        df.withColumn(ID_COLUMN, expr(idExpr))

      case None if isAggregated =>
        // Since concat doesn't support struct or map type, convert these to json which is more
        // deterministic than casting to string, as its format may vary across Spark versions.
        val allOutputCols = df.schema.fields.map { field =>
          field.dataType match {
            case _: StructType | _: MapType =>
              to_json(col(field.name))
            case _ =>
              col(field.name)
          }
        }

        // TODO: 1) use only grouping columns; 2) ensure aggregation is on top level
        df.withColumn(ID_COLUMN, sha1(concat_ws("\0", allOutputCols: _*)))

      case _ => df
    }
  }

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

    // Optional latest metadata log entry
    val latestLogEntry = index.latestLogEntry
    if (latestLogEntry.isDefined) {
      builder.latestLogEntry(latestLogEntry.get)
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

  def generateSchema(allFieldTypes: Map[String, String]): Map[String, AnyRef] = {
    val schemaJson = generateSchemaJSON(allFieldTypes)
    var schemaMap: Map[String, AnyRef] = Map.empty

    parseJson(schemaJson) { (parser, fieldName) =>
      fieldName match {
        case "properties" => schemaMap = parser.map().asScala.toMap
        case _ => // do nothing
      }
    }

    schemaMap
  }
}
