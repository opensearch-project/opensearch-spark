/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.table

import java.util.logging.Logger

import scala.collection.JavaConverters._

import org.json4s.{Formats, NoTypeHints}
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods
import org.json4s.native.Serialization
import org.opensearch.client.opensearch._types.Time
import org.opensearch.client.opensearch.core.pit.CreatePitRequest
import org.opensearch.client.opensearch.indices.IndicesStatsRequest
import org.opensearch.client.opensearch.indices.stats.IndicesStats
import org.opensearch.flint.core.{FlintClientBuilder, FlintOptions}
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchClientUtils}
import org.opensearch.flint.table.OpenSearchIndexTable.LOG

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
      val docCount = indexStats.total().docs().count()
      if (docCount == 0) {
        maxResultWindow
      } else {
        val totalSizeBytes = indexStats.total().store().sizeInBytes
        val docSize = Math.ceil(totalSizeBytes / docCount).toLong
        val maxSplitSizeBytes = 10 * 1024 * 1024
        Math.max(Math.min(maxSplitSizeBytes / docSize, maxResultWindow), 1).toInt
      }
    }
  }

  /**
   * The number of shards in the index.
   */
  lazy val numberOfShards: Int =
    (JsonMethods.parse(metaData.setting) \ "index.number_of_shards").extract[String].toInt

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
   * Creates a snapshot of the index using PIT.
   *
   * @return
   *   A new OpenSearchIndexSnapshotTable instance.
   */
  override def snapshot(): Table = {
    val response = OpenSearchClientUtils
      .createClient(option)
      .createPit(
        new CreatePitRequest.Builder()
          .targetIndexes(name)
          .keepAlive(new Time.Builder().time(option.getScrollDuration.toString + "m").build)
          .build)
    LOG.info("create PIT: " + response.pitId())
    new OpenSearchIndexSnapshotTable(metaData, option, response.pitId())
  }

  /**
   * Slices the table. Not supported for OpenSearchIndexTable.
   *
   * @throws UnsupportedOperationException
   *   if this method is called.
   */
  override def slice(): Seq[Table] = {
    throw new UnsupportedOperationException("Can't slice on OpenSearchIndexTable")
  }

  /**
   * Creates a reader for the table. Not supported for OpenSearchIndexTable.
   *
   * @param query
   *   The query string.
   * @return
   *   A FlintReader instance.
   * @throws UnsupportedOperationException
   *   if this method is called.
   */
  override def createReader(query: String): FlintReader = {
    throw new UnsupportedOperationException("Can't create reader on OpenSearchIndexTable")
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
}

object OpenSearchIndexTable {
  private val LOG = Logger.getLogger(classOf[OpenSearchIndexTable].getName)

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
   * @return
   *   An list of OpenSearchIndexTable instance.
   */
  def apply(indexName: String, options: FlintOptions): Seq[OpenSearchIndexTable] = {
    val client = FlintClientBuilder.build(options)
    client
      .getAllIndexMetadata(indexName.split(","): _*)
      .asScala
      .toMap
      .map(entry => {
        new OpenSearchIndexTable(MetaData.apply(entry._1, entry._2), options)
      })
      .toSeq
  }
}
