/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.table

import scala.collection.JavaConverters._

import org.json4s.{Formats, NoTypeHints}
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods
import org.json4s.native.Serialization
import org.opensearch.action.search.SearchRequest
import org.opensearch.client.opensearch.indices.IndicesStatsRequest
import org.opensearch.client.opensearch.indices.stats.IndicesStats
import org.opensearch.flint.core._
import org.opensearch.flint.core.metadata.FlintIndexMetadataServiceBuilder
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchClientUtils, OpenSearchSearchAfterQueryReader}
import org.opensearch.flint.core.table.OpenSearchIndexTable.maxSplitSizeBytes
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder

import org.apache.spark.SparkConf

/**
 * Represents an OpenSearch index.
 *
 * @param metaData
 *   MetaData containing information about the OpenSearch index.
 * @param option
 *   FlintOptions containing configuration options for the Flint client.
 */
class OpenSearchIndexTable(metaData: MetaData, option: FlintOptions) extends Table {
  @transient implicit val formats: Formats = Serialization.formats(NoTypeHints)

  /**
   * The name of the index.
   */
  lazy val name: String = metaData.name

  /**
   * The index stats.
   */
  lazy val indexStats: IndicesStats =
    OpenSearchClientUtils
      .createClient(option)
      .stats(new IndicesStatsRequest.Builder().index(name).build())
      .indices()
      .get(name)

  /**
   * The page size for OpenSearch Rest Request.
   */
  lazy val pageSize: Int = {
    if (option.getScrollSize.isPresent) {
      option.getScrollSize.get()
    } else {
      val docCount = indexStats.primaries().docs().count()
      if (docCount == 0) {
        maxResultWindow
      } else {
        val totalSizeBytes = indexStats.primaries().store().sizeInBytes
        val docSize = Math.ceil(totalSizeBytes / docCount).toLong
        Math.max(Math.min(maxSplitSizeBytes / docSize, maxResultWindow), 1).toInt
      }
    }
  }

  /**
   * The number of shards in the index.
   */
  lazy val numberOfShards: Int = {
    if (option.supportShard()) {
      (JsonMethods.parse(metaData.setting) \ "index.number_of_shards").extract[String].toInt
    } else {
      1
    }
  }

  /**
   * The maximum result window for the index.
   */
  lazy val maxResultWindow: Int = {
    (JsonMethods.parse(metaData.setting) \ "index.max_result_window") match {
      case JString(value) => value.toInt
      case _ => 10000
    }
  }

  /**
   * Slices the table.
   */
  override def slice(): Seq[Table] = {
    Range(0, numberOfShards).map(shardId =>
      new OpenSearchIndexShardTable(metaData, option, shardId))
  }

  /**
   * Creates a reader for the table. Not supported for OpenSearchIndexTable.
   *
   * @param query
   *   The query string.
   * @return
   *   A FlintReader instance.
   */
  override def createReader(query: String): FlintReader = {
    new OpenSearchSearchAfterQueryReader(
      OpenSearchClientUtils.createClient(option),
      new SearchRequest()
        .indices(name)
        .source(
          new SearchSourceBuilder()
            .query(Table.queryBuilder(query))
            .size((pageSize))
            .sort("_doc", SortOrder.ASC)
            .sort("_id", SortOrder.ASC)))
  }

  /**
   * Returns the schema of the table.
   *
   * @return
   *   A Schema instance.
   */
  override def schema(): Schema = JsonSchema(metaData.properties)

  /**
   * Returns the metadata of the table.
   *
   * @return
   *   A MetaData instance.
   */
  override def metaData(): MetaData = metaData

  /**
   * Is OpenSearch Table splittable.
   *
   * @return
   *   true if splittable, otherwise false.
   */
  override def isSplittable(): Boolean = numberOfShards > 1
}

object OpenSearchIndexTable {

  /**
   * Max OpenSearch Request Page size is 10MB.
   */
  val maxSplitSizeBytes = 10 * 1024 * 1024
}

object OpenSearchCluster {

  /**
   * Creates list of OpenSearchIndexTable instance of indices in OpenSearch domain.
   *
   * @param indexName
   *   tableName support (1) single index name. (2) wildcard index name. (3) comma sep index name.
   * @param options
   *   The options for Flint.
   * @param conf
   *   Configurations for Spark application.
   * @return
   *   An list of OpenSearchIndexTable instance.
   */
  def apply(
      indexName: String,
      options: FlintOptions,
      conf: SparkConf): Seq[OpenSearchIndexTable] = {
    val indexMetadataService = FlintIndexMetadataServiceBuilder.build(options, conf)
    indexMetadataService
      .getAllIndexMetadata(indexName.split(","): _*)
      .asScala
      .toMap
      .map(entry => {
        new OpenSearchIndexTable(MetaData.apply(entry._1, entry._2), options)
      })
      .toSeq
  }

  /**
   * Use for testing only.
   */
  def apply(indexName: String, options: FlintOptions): Seq[OpenSearchIndexTable] = {
    apply(indexName, options, new SparkConf())
  }
}
