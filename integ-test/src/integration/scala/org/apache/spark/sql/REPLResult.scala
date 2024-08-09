/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.util.Try

import org.json4s.{DefaultFormats, Formats}
import org.json4s.native.JsonMethods.parse

class REPLResult(
    val results: Seq[String],
    val schemas: Seq[String],
    val jobRunId: String,
    val applicationId: String,
    val dataSourceName: String,
    val status: String,
    val error: String,
    val queryId: String,
    val queryText: String,
    val sessionId: String,
    val updateTime: Long,
    val queryRunTime: Long) {
  override def toString: String = {
    s"REPLResult(results=$results, schemas=$schemas, jobRunId=$jobRunId, applicationId=$applicationId, " +
      s"dataSourceName=$dataSourceName, status=$status, error=$error, queryId=$queryId, queryText=$queryText, " +
      s"sessionId=$sessionId, updateTime=$updateTime, queryRunTime=$queryRunTime)"
  }
}

object REPLResult {
  implicit val formats: Formats = DefaultFormats

  def deserialize(jsonString: String): Try[REPLResult] = Try {
    val json = parse(jsonString)

    new REPLResult(
      results = (json \ "result").extract[Seq[String]],
      schemas = (json \ "schema").extract[Seq[String]],
      jobRunId = (json \ "jobRunId").extract[String],
      applicationId = (json \ "applicationId").extract[String],
      dataSourceName = (json \ "dataSourceName").extract[String],
      status = (json \ "status").extract[String],
      error = (json \ "error").extract[String],
      queryId = (json \ "queryId").extract[String],
      queryText = (json \ "queryText").extract[String],
      sessionId = (json \ "sessionId").extract[String],
      updateTime = (json \ "updateTime").extract[Long],
      queryRunTime = (json \ "queryRunTime").extract[Long])
  }
}
