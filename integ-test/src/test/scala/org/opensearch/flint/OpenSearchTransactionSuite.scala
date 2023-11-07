/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint

import java.util.Collections

import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.update.UpdateRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState.IndexState
import org.opensearch.flint.core.storage.FlintOpenSearchClient._
import org.opensearch.flint.spark.FlintSparkSuite

/**
 * Transaction test base suite that creates the metadata log index which enables transaction
 * support in index operation.
 */
trait OpenSearchTransactionSuite extends FlintSparkSuite {

  val testDataSourceName = "myglue"
  lazy val testMetaLogIndex: String = META_LOG_NAME_PREFIX + "_" + testDataSourceName

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.flint.datasource.name", testDataSourceName)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    openSearchClient
      .indices()
      .create(new CreateIndexRequest(testMetaLogIndex), RequestOptions.DEFAULT)
  }

  override def afterEach(): Unit = {
    openSearchClient
      .indices()
      .delete(new DeleteIndexRequest(testMetaLogIndex), RequestOptions.DEFAULT)
    super.afterEach()
  }

  def latestLogEntry(latestId: String): Map[String, AnyRef] = {
    val response = openSearchClient
      .get(new GetRequest(testMetaLogIndex, latestId), RequestOptions.DEFAULT)

    Option(response.getSourceAsMap).getOrElse(Collections.emptyMap()).asScala.toMap
  }

  def createLatestLogEntry(latest: FlintMetadataLogEntry): Unit = {
    openSearchClient.index(
      new IndexRequest()
        .index(testMetaLogIndex)
        .id(latest.id)
        .source(latest.toJson, XContentType.JSON),
      RequestOptions.DEFAULT)
  }

  def updateLatestLogEntry(latest: FlintMetadataLogEntry, newState: IndexState): Unit = {
    openSearchClient.update(
      new UpdateRequest(testMetaLogIndex, latest.id)
        .doc(latest.copy(state = newState).toJson, XContentType.JSON),
      RequestOptions.DEFAULT)
  }
}
