/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.mv

import org.json4s.{Formats, NoTypeHints}
import org.json4s.JsonAST.{JArray, JObject, JString}
import org.json4s.native.JsonMethods.{compact, parse, render}
import org.json4s.native.Serialization
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndex, FlintSparkIndexBuilder, FlintSparkIndexOptions}
import org.opensearch.flint.spark.FlintSparkIndexOptions.empty
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.{getFlintIndexName, MV_INDEX_TYPE}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.flint.datatype.FlintDataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Flint materialized view in Spark.
 *
 * @param mvName
 *   materialized view name
 * @param query
 *   unresolved plan
 */
case class FlintSparkMaterializedView(
    mvName: String,
    query: String,
    outputSchema: Map[String, String],
    override val options: FlintSparkIndexOptions = empty)
    extends FlintSparkIndex {

  /** Required by json4s write function */
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  /** TODO: add it to index option */
  private val watermarkDelay = UTF8String.fromString("0 Minute")

  override val kind: String = MV_INDEX_TYPE

  override def name(): String = getFlintIndexName(mvName)

  override def metadata(): FlintMetadata =
    new FlintMetadata(s"""{
         |   "_meta": {
         |     "name": "$mvName",
         |     "kind": "$kind",
         |     "indexedColumns": $getMetaInfo,
         |     "source": $getEscapedQuery,
         |     "options": $getIndexOptions
         |   },
         |   "properties": $getSchema
         | }
         |""".stripMargin)

  private def getMetaInfo: String = {
    val objects = outputSchema.map { case (colName, colType) =>
      JObject("columnName" -> JString(colName), "columnType" -> JString(colType))
    }.toList
    Serialization.write(JArray(objects))
  }

  private def getEscapedQuery: String = {
    compact(render(JString(query)))
  }

  private def getIndexOptions: String = {
    Serialization.write(options.options)
  }

  private def getSchema: String = {
    val catalogDDL =
      outputSchema
        .map { case (colName, colType) => s"$colName $colType not null" }
        .mkString(",")
    val properties = FlintDataType.serialize(StructType.fromDDL(catalogDDL))
    compact(render(parse(properties) \ "properties"))
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
      // TODO: need to change this and Flint DS to support complext field type
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
