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
import org.opensearch.flint.spark.FlintSparkSuite

trait OpenSearchTransactionSuite {
  self: FlintSparkSuite =>

  val testMetadataLogIndex = ".query_request_history_mys3"

  override def beforeEach(): Unit = {
    openSearchClient
      .indices()
      .create(new CreateIndexRequest(testMetadataLogIndex), RequestOptions.DEFAULT)
  }

  override def afterEach(): Unit = {
    openSearchClient
      .indices()
      .delete(new DeleteIndexRequest(testMetadataLogIndex), RequestOptions.DEFAULT)
  }

  def latestLogEntry(latestId: String): Map[String, AnyRef] = {
    val response = openSearchClient
      .get(new GetRequest(testMetadataLogIndex, latestId), RequestOptions.DEFAULT)

    Option(response.getSourceAsMap).getOrElse(Collections.emptyMap()).asScala.toMap
  }

  def createLatestLogEntry(latest: FlintMetadataLogEntry): Unit = {
    openSearchClient.index(
      new IndexRequest()
        .index(testMetadataLogIndex)
        .id(latest.id)
        .source(latest.toJson, XContentType.JSON),
      RequestOptions.DEFAULT)
  }

  def updateLatestLogEntry(latest: FlintMetadataLogEntry, newState: IndexState): Unit = {
    openSearchClient.update(
      new UpdateRequest(testMetadataLogIndex, latest.id)
        .doc(latest.copy(state = newState).toJson, XContentType.JSON),
      RequestOptions.DEFAULT)
  }
}
