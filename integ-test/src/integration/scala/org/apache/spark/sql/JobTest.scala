/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}
import scala.util.control.Breaks._

import org.opensearch.OpenSearchStatusException
import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.core.FlintOptions
import org.opensearch.search.sort.SortOrder

import org.apache.spark.internal.Logging

/**
 * We use a self-type annotation (self: OpenSearchSuite =>) to specify that it must be mixed into
 * a class that also mixes in OpenSearchSuite. This way, JobTest can still use the
 * openSearchOptions field,
 */
trait JobTest extends Logging { self: OpenSearchSuite =>

  def pollForResultAndAssert(
      osClient: OSClient,
      expected: REPLResult => Boolean,
      idField: String,
      idValue: String,
      timeoutMillis: Long,
      resultIndex: String): Unit = {
    val query =
      s"""{
         |  "bool": {
         |    "must": [
         |      {
         |        "term": {
         |          "$idField": "$idValue"
         |        }
         |      }
         |    ]
         |  }
         |}""".stripMargin
    val resultReader = osClient.createQueryReader(resultIndex, query, "updateTime", SortOrder.ASC)

    val startTime = System.currentTimeMillis()
    breakable {
      while (System.currentTimeMillis() - startTime < timeoutMillis) {
        logInfo(s"Check result for $idValue")
        try {
          if (resultReader.hasNext()) {
            REPLResult.deserialize(resultReader.next()) match {
              case Success(replResult) =>
                logInfo(s"repl result: $replResult")
                assert(expected(replResult), s"{$query} failed.")
                verifyFastRefresh(resultIndex)
              case Failure(exception) =>
                assert(false, "Failed to deserialize: " + exception.getMessage)
            }
            break
          }
        } catch {
          case e: OpenSearchStatusException => logError("Exception while querying for result", e)
        }

        Thread.sleep(2000) // 2 seconds
      }
      if (System.currentTimeMillis() - startTime >= timeoutMillis) {
        assert(
          false,
          s"Timeout occurred after $timeoutMillis milliseconds waiting for query result.")
      }
    }
  }

  /**
   * Used to preprocess multi-line queries before comparing them as serialized and deserialized
   * queries might have different characters.
   * @param s
   *   input
   * @return
   *   normalized input by replacing all space, tab, ane newlines with single spaces.
   */
  def normalizeString(s: String): String = {
    // \\s+ is a regular expression that matches one or more whitespace characters, including spaces, tabs, and newlines.
    s.replaceAll("\\s+", " ")
  } // Replace all whitespace characters with empty string

  def verifyFastRefresh(resultIndex: String): Unit = {
    val fastRefreshSettingKey = "refresh_interval"
    val fastRefreshSettingVal = "1s"

    val getIndexResponse = getIndex(resultIndex)
    val indexToSettings = getIndexResponse.getSettings
    assert(indexToSettings.containsKey(resultIndex))
    val settings = indexToSettings.get(resultIndex)
    throw new IllegalArgumentException(
      s"result index: $resultIndex, settings: ${settings.toString}")
    assert(settings.hasValue(fastRefreshSettingKey))
    assert(settings.get(fastRefreshSettingKey) == fastRefreshSettingVal)
  }
}
