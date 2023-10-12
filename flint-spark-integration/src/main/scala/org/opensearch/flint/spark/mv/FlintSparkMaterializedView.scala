/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.mv

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndex, FlintSparkIndexBuilder, FlintSparkIndexOptions}
import org.opensearch.flint.spark.FlintSparkIndex.{generateSchemaJSON, metadataBuilder}
import org.opensearch.flint.spark.FlintSparkIndexOptions.empty
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.{getFlintIndexName, MV_INDEX_TYPE}

import org.apache.spark.sql.DataFrame
import org.apache.spark.unsafe.types.UTF8String

case class FlintSparkMaterializedView(
    mvName: String,
    query: String,
    outputSchema: Map[String, String],
    override val options: FlintSparkIndexOptions = empty)
    extends FlintSparkIndex {

  /** TODO: add it to index option */
  private val watermarkDelay = UTF8String.fromString("0 Minute")

  override val kind: String = MV_INDEX_TYPE

  override def name(): String = getFlintIndexName(mvName)

  override def metadata(): FlintMetadata = {
    val indexColumnMaps =
      outputSchema.map { case (colName, colType) =>
        Map[String, AnyRef]("columnName" -> colName, "columnType" -> colType).asJava
      }.toArray
    val schemaJson = generateSchemaJSON(outputSchema)

    metadataBuilder(this)
      .name(mvName)
      .source(query)
      .indexedColumns(indexColumnMaps)
      .schema(schemaJson)
      .build()
  }

  override def build(df: DataFrame): DataFrame = {
    null
  }
}

object FlintSparkMaterializedView {

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
    require(mvName.contains("."), "Full table name database.mv is required")

    s"flint_${mvName.replace(".", "_")}"
  }

  /** Builder class for MV build */
  class Builder(flint: FlintSpark) extends FlintSparkIndexBuilder(flint) {
    private var mvName: String = ""
    private var query: String = ""

    /**
     * Set MV name.
     *
     * @param mvName
     *   MV name
     * @return
     *   builder
     */
    def name(mvName: String): Builder = {
      this.mvName = mvName
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
      this
    }

    override protected def buildIndex(): FlintSparkIndex = {
      // TODO: need to change this and Flint DS to support complex field type
      val outputSchema = flint.spark
        .sql(query)
        .schema
        .map { field =>
          field.name -> field.dataType.typeName
        }
        .toMap
      FlintSparkMaterializedView(mvName, query, outputSchema, indexOptions)
    }
  }
}
