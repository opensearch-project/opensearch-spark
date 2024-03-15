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
import org.opensearch.client.indices.{CreateIndexRequest, GetIndexRequest}
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.{QUERY_EXECUTION_REQUEST_MAPPING, QUERY_EXECUTION_REQUEST_SETTINGS}
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState.IndexState
import org.opensearch.flint.core.storage.FlintOpenSearchClient._
import org.opensearch.flint.spark.FlintSparkSuite

import org.apache.spark.sql.flint.config.FlintSparkConf.DATA_SOURCE_NAME

/**
 * Transaction test base suite that creates the metadata log index which enables transaction
 * support in index operation.
 */
trait OpenSearchTransactionSuite extends FlintSparkSuite {

  val testDataSourceName = "myglue"
  lazy val testMetaLogIndex: String = META_LOG_NAME_PREFIX + "_" + testDataSourceName

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DATA_SOURCE_NAME.key, testDataSourceName)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    openSearchClient
      .indices()
      .create(
        new CreateIndexRequest(testMetaLogIndex)
          .mapping(QUERY_EXECUTION_REQUEST_MAPPING, XContentType.JSON)
          .settings(QUERY_EXECUTION_REQUEST_SETTINGS, XContentType.JSON),
        RequestOptions.DEFAULT)
  }

  override def afterEach(): Unit = {
    deleteIndex(testMetaLogIndex)
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

  def deleteIndex(indexName: String): Unit = {
    if (openSearchClient
        .indices()
        .exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
      openSearchClient
        .indices()
        .delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT)
    }
  }

  def indexMapping(): String = {
    val response =
      openSearchClient.indices.get(new GetIndexRequest(testMetaLogIndex), RequestOptions.DEFAULT)

    response.getMappings.get(testMetaLogIndex).source().toString
  }
}
