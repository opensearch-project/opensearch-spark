/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.{CreateIndexRequest, GetIndexRequest, GetIndexResponse}
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.core.{FlintClientBuilder, FlintOptions}

import org.apache.spark.internal.Logging

class OSClient(val flintOptions: FlintOptions) extends Logging {

  def getIndexMetadata(osIndexName: String): String = {

    using(FlintClientBuilder.build(flintOptions).createClient()) { client =>
      val request = new GetIndexRequest(osIndexName)
      try {
        val response = client.indices.get(request, RequestOptions.DEFAULT)
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
  def createIndex(osIndexName: String, mapping: String): Either[String, Unit] = {
    logInfo(s"create $osIndexName")

    using(FlintClientBuilder.build(flintOptions).createClient()) { client =>
      val request = new CreateIndexRequest(osIndexName)
      request.mapping(mapping, XContentType.JSON)

      try {
        client.indices.create(request, RequestOptions.DEFAULT)
        logInfo(s"create $osIndexName successfully")
        Right(())
      } catch {
        case e: Exception =>
          val error = s"Failed to create result index $osIndexName"
          logError(error, e)
          Left(error)
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

}
