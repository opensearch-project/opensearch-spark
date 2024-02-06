/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.io.IOException
import java.util.ArrayList
import java.util.Locale

import org.opensearch.action.get.{GetRequest, GetResponse}
import org.opensearch.client.{RequestOptions, RestHighLevelClient}
import org.opensearch.client.indices.{CreateIndexRequest, GetIndexRequest, GetIndexResponse}
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.common.Strings
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.{NamedXContentRegistry, XContentParser, XContentType}
import org.opensearch.common.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder, FlintOptions}
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchScrollReader, OpenSearchUpdater}
import org.opensearch.index.query.{AbstractQueryBuilder, MatchAllQueryBuilder, QueryBuilder}
import org.opensearch.plugins.SearchPlugin
import org.opensearch.search.SearchModule
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder

import org.apache.spark.internal.Logging

class OSClient(val flintOptions: FlintOptions) extends Logging {
  val flintClient: FlintClient = FlintClientBuilder.build(flintOptions)

  /**
   * {@link NamedXContentRegistry} from {@link SearchModule} used for construct {@link
   * QueryBuilder} from DSL query string.
   */
  private val xContentRegistry: NamedXContentRegistry = new NamedXContentRegistry(
    new SearchModule(Settings.builder.build, new ArrayList[SearchPlugin]).getNamedXContents)
  def getIndexMetadata(osIndexName: String): String = {

    using(flintClient.createClient()) { client =>
      val request = new GetIndexRequest(osIndexName)
      try {
        val response = client.getIndex(request, RequestOptions.DEFAULT)
        response.getMappings.get(osIndexName).source.string
      } catch {
        case e: Exception =>
          throw new IllegalStateException(
            s"Failed to get OpenSearch index mapping for $osIndexName",
            e)
      }
    }
  }

  /**
   * Create a new index with given mapping.
   *
   * @param osIndexName
   *   the name of the index
   * @param mapping
   *   the mapping of the index
   * @return
   *   use Either for representing success or failure. A Right value indicates success, while a
   *   Left value indicates an error.
   */
  def createIndex(osIndexName: String, mapping: String): Unit = {
    logInfo(s"create $osIndexName")

    using(flintClient.createClient()) { client =>
      val request = new CreateIndexRequest(osIndexName)
      request.mapping(mapping, XContentType.JSON)

      try {
        client.createIndex(request, RequestOptions.DEFAULT)
        logInfo(s"create $osIndexName successfully")
      } catch {
        case e: Exception =>
          throw new IllegalStateException(s"Failed to create index $osIndexName", e);
      }
    }
  }

  /**
   * the loan pattern to manage resource.
   *
   * @param resource
   *   the resource to be managed
   * @param f
   *   the function to be applied to the resource
   * @tparam A
   *   the type of the resource
   * @tparam B
   *   the type of the result
   * @return
   *   the result of the function
   */
  def using[A <: AutoCloseable, B](resource: A)(f: A => B): B = {
    try {
      f(resource)
    } finally {
      // client is guaranteed to be non-null
      resource.close()
    }
  }

  def createUpdater(indexName: String): OpenSearchUpdater =
    new OpenSearchUpdater(indexName, flintClient)

  def getDoc(osIndexName: String, id: String): GetResponse = {
    using(flintClient.createClient()) { client =>
      try {
        val request = new GetRequest(osIndexName, id)
        client.get(request, RequestOptions.DEFAULT)
      } catch {
        case e: Exception =>
          throw new IllegalStateException(
            String.format(
              Locale.ROOT,
              "Failed to retrieve doc %s from index %s",
              osIndexName,
              id),
            e)
      }
    }
  }

  def createReader(indexName: String, query: String, sort: String): FlintReader = try {
    var queryBuilder: QueryBuilder = new MatchAllQueryBuilder
    if (!Strings.isNullOrEmpty(query)) {
      val parser =
        XContentType.JSON.xContent.createParser(xContentRegistry, IGNORE_DEPRECATIONS, query)
      queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(parser)
    }
    new OpenSearchScrollReader(
      flintClient.createClient(),
      indexName,
      new SearchSourceBuilder().query(queryBuilder).sort(sort, SortOrder.ASC),
      flintOptions)
  } catch {
    case e: IOException =>
      throw new RuntimeException(e)
  }

  def doesIndexExist(indexName: String): Boolean = {
    using(flintClient.createClient()) { client =>
      try {
        val request = new GetIndexRequest(indexName)
        client.isIndexExists(request, RequestOptions.DEFAULT)
      } catch {
        case e: Exception =>
          throw new IllegalStateException(s"Failed to check if index $indexName exists", e)
      }
    }
  }
}
