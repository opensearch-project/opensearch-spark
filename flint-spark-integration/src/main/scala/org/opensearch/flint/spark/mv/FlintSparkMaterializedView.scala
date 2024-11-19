/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.mv

import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`map AsScala`

import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndex, FlintSparkIndexBuilder, FlintSparkIndexOptions}
import org.opensearch.flint.spark.FlintSparkIndex.{flintIndexNamePrefix, generateSchema, metadataBuilder, StreamingRefresh}
import org.opensearch.flint.spark.FlintSparkIndexOptions.empty
import org.opensearch.flint.spark.function.TumbleFunction
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.{getFlintIndexName, MV_INDEX_TYPE}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, EventTimeWatermark, LogicalPlan}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.flint.{logicalPlanToDataFrame, qualifyTableName}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Flint materialized view in Spark.
 *
 * @param mvName
 *   MV name
 * @param query
 *   source query that generates MV data
 * @param sourceTables
 *   source table names
 * @param outputSchema
 *   output schema
 * @param options
 *   index options
 * @param latestLogEntry
 *   latest metadata log entry for index
 */
case class FlintSparkMaterializedView(
    mvName: String,
    query: String,
    sourceTables: Array[String],
    outputSchema: Map[String, String],
    override val options: FlintSparkIndexOptions = empty,
    override val latestLogEntry: Option[FlintMetadataLogEntry] = None)
    extends FlintSparkIndex
    with StreamingRefresh {

  override val kind: String = MV_INDEX_TYPE

  override def name(): String = getFlintIndexName(mvName)

  override def metadata(): FlintMetadata = {
    val indexColumnMaps =
      outputSchema.map { case (colName, colType) =>
        Map[String, AnyRef]("columnName" -> colName, "columnType" -> colType).asJava
      }.toArray
    val schema = generateSchema(outputSchema).asJava

    // Convert Scala Array to Java ArrayList for consistency with OpenSearch JSON parsing.
    // OpenSearch uses Jackson, which deserializes JSON arrays into ArrayLists.
    val sourceTablesProperty = new java.util.ArrayList[String](sourceTables.toSeq.asJava)

    metadataBuilder(this)
      .name(mvName)
      .source(query)
      .addProperty("sourceTables", sourceTablesProperty)
      .indexedColumns(indexColumnMaps)
      .schema(schema)
      .build()
  }

  override def build(spark: SparkSession, df: Option[DataFrame]): DataFrame = {
    require(df.isEmpty, "materialized view doesn't support reading from other data frame")

    spark.sql(query)
  }

  override def buildStream(spark: SparkSession): DataFrame = {
    val batchPlan = spark.sql(query).queryExecution.logical

    /*
     * Convert unresolved batch plan to streaming plan by:
     *  1.Insert Watermark operator below Aggregate (required by Spark streaming)
     *  2.Set isStreaming flag to true in Relation operator
     */
    val streamingPlan = batchPlan transform {
      case WindowingAggregate(aggregate, timeCol) =>
        aggregate.copy(child = watermark(timeCol, aggregate.child))

      case relation: UnresolvedRelation if !relation.isStreaming =>
        relation.copy(isStreaming = true, options = optionsWithExtra(spark, relation))
    }
    logicalPlanToDataFrame(spark, streamingPlan)
  }

  private def watermark(timeCol: Attribute, child: LogicalPlan) = {
    require(
      options.watermarkDelay().isDefined,
      "watermark delay is required for auto refresh and incremental refresh with aggregation")

    val delay = options.watermarkDelay().get
    EventTimeWatermark(timeCol, IntervalUtils.fromIntervalString(delay), child)
  }

  private def optionsWithExtra(
      spark: SparkSession,
      relation: UnresolvedRelation): CaseInsensitiveStringMap = {
    val originalOptions = relation.options.asCaseSensitiveMap
    val tableName = qualifyTableName(spark, relation.tableName)
    val extraOptions = options.extraSourceOptions(tableName).asJava
    new CaseInsensitiveStringMap((originalOptions ++ extraOptions).asJava)
  }

  /**
   * Extractor that extract event time column out of Aggregate operator.
   */
  private object WindowingAggregate {

    def unapply(agg: Aggregate): Option[(Aggregate, Attribute)] = {
      val winFuncs = agg.groupingExpressions.collect {
        case func: UnresolvedFunction if isWindowingFunction(func) =>
          func
      }

      if (winFuncs.size != 1) {
        throw new IllegalStateException(
          "A windowing function is required for incremental refresh with aggregation")
      }

      // Assume first aggregate item must be time column
      val winFunc = winFuncs.head
      val timeCol = winFunc.arguments.head
      timeCol match {
        case attr: Attribute =>
          Some(agg, attr)
        case _ =>
          throw new IllegalArgumentException(
            s"Tumble function only supports simple timestamp column, but found: $timeCol")
      }
    }

    private def isWindowingFunction(func: UnresolvedFunction): Boolean = {
      val funcName = func.nameParts.mkString(".").toLowerCase(Locale.ROOT)
      val funcIdent = FunctionIdentifier(funcName)

      // TODO: support other window functions
      funcIdent == TumbleFunction.identifier
    }
  }
}

object FlintSparkMaterializedView extends Logging {

  /** MV index type name */
  val MV_INDEX_TYPE = "mv"

  /**
   * Get index name following the convention "flint_" + qualified MV name (replace dot with
   * underscore).
   *
   * @param mvName
   *   MV name
   * @return
   *   Flint index name
   */
  def getFlintIndexName(mvName: String): String = {
    require(
      mvName.split("\\.").length >= 3,
      "Qualified materialized view name catalog.database.mv is required")

    flintIndexNamePrefix(mvName)
  }

  /**
   * Extract source table names (possibly more than one) from the query.
   *
   * @param spark
   *   Spark session
   * @param query
   *   source query that generates MV data
   * @return
   *   source table names
   */
  def extractSourceTablesFromQuery(spark: SparkSession, query: String): Array[String] = {
    logInfo(s"Extracting source tables from query $query")
    val sourceTables = spark.sessionState.sqlParser
      .parsePlan(query)
      .collect { case relation: UnresolvedRelation =>
        qualifyTableName(spark, relation.tableName)
      }
      .toArray
    logInfo(s"Extracted tables: [${sourceTables.mkString(", ")}]")
    sourceTables
  }

  /**
   * Get source tables from Flint metadata properties field.
   *
   * @param metadata
   *   Flint metadata
   * @return
   *   source table names
   */
  def getSourceTablesFromMetadata(metadata: FlintMetadata): Array[String] = {
    logInfo(s"Getting source tables from metadata $metadata")
    val sourceTables = metadata.properties.get("sourceTables")
    sourceTables match {
      case list: java.util.ArrayList[_] =>
        logInfo(s"sourceTables is [${list.asScala.mkString(", ")}]")
        list.toArray.map(_.toString)
      case null =>
        logInfo("sourceTables property does not exist")
        Array.empty[String]
      case _ =>
        logInfo(s"sourceTables has unexpected type: ${sourceTables.getClass.getName}")
        Array.empty[String]
    }
  }

  /** Builder class for MV build */
  class Builder(flint: FlintSpark) extends FlintSparkIndexBuilder(flint) {
    private var mvName: String = ""
    private var query: String = ""
    private var sourceTables: Array[String] = Array.empty[String]

    /**
     * Set MV name.
     *
     * @param mvName
     *   MV name
     * @return
     *   builder
     */
    def name(mvName: String): Builder = {
      this.mvName = qualifyTableName(flint.spark, mvName)
      this
    }

    /**
     * Set MV query.
     *
     * @param query
     *   MV query
     * @return
     *   builder
     */
    def query(query: String): Builder = {
      this.query = query
      this.sourceTables = extractSourceTablesFromQuery(flint.spark, query)
      this
    }

    override protected def validateIndex(index: FlintSparkIndex): FlintSparkIndex = {
      /*
       * Validate if duplicate column names in the output schema.
       * MV query may be empty in the case of ALTER index statement.
       */
      if (query.nonEmpty) {
        val outputColNames = flint.spark.sql(query).schema.map(_.name)
        require(
          outputColNames.distinct.length == outputColNames.length,
          "Duplicate columns found in materialized view query output")
      }

      // Continue to perform any additional index validation
      super.validateIndex(index)
    }

    override protected def buildIndex(): FlintSparkIndex = {
      // TODO: change here and FlintDS class to support complex field type in future
      val outputSchema = flint.spark
        .sql(query)
        .schema
        .map { field =>
          field.name -> field.dataType.simpleString
        }
        .toMap
      FlintSparkMaterializedView(mvName, query, sourceTables, outputSchema, indexOptions)
    }
  }
}
