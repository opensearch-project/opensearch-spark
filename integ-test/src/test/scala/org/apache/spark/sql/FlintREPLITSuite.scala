/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.util.UUID

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MINUTES}
import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks.{break, breakable}

import org.opensearch.OpenSearchStatusException
import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.app.{FlintCommand, FlintInstance}
import org.opensearch.flint.core.{FlintClient, FlintOptions}
import org.opensearch.flint.core.storage.{FlintOpenSearchClient, FlintReader, OpenSearchUpdater}
import org.opensearch.search.sort.SortOrder

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.flint.config.FlintSparkConf.{DATA_SOURCE_NAME, EXCLUDE_JOB_IDS, HOST_ENDPOINT, HOST_PORT, JOB_TYPE, REFRESH_POLICY, REPL_INACTIVITY_TIMEOUT_MILLIS, REQUEST_INDEX, SESSION_ID}
import org.apache.spark.sql.util.MockEnvironment
import org.apache.spark.util.ThreadUtils

class FlintREPLITSuite extends SparkFunSuite with OpenSearchSuite with JobTest {

  var flintClient: FlintClient = _
  var osClient: OSClient = _
  var updater: OpenSearchUpdater = _
  val requestIndex = "flint_ql_sessions"
  val resultIndex = "query_results2"
  val jobRunId = "00ff4o3b5091080q"
  val appId = "00feq82b752mbt0p"
  val dataSourceName = "my_glue1"
  val sessionId = "10"
  val requestIndexMapping =
    """ {
      |   "properties": {
      |     "applicationId": {
      |          "type": "keyword"
      |        },
      |        "dataSourceName": {
      |          "type": "keyword"
      |        },
      |        "error": {
      |          "type": "text"
      |        },
      |        "excludeJobIds": {
      |          "type": "text",
      |          "fields": {
      |            "keyword": {
      |              "type": "keyword",
      |              "ignore_above": 256
      |            }
      |          }
      |        },
      |        "if_primary_term": {
      |          "type": "long"
      |        },
      |        "if_seq_no": {
      |          "type": "long"
      |        },
      |        "jobId": {
      |          "type": "keyword"
      |        },
      |        "jobStartTime": {
      |          "type": "long"
      |        },
      |        "lang": {
      |          "type": "keyword"
      |        },
      |        "lastUpdateTime": {
      |          "type": "date",
      |          "format": "strict_date_time||epoch_millis"
      |        },
      |        "query": {
      |          "type": "text"
      |        },
      |        "queryId": {
      |          "type": "text",
      |          "fields": {
      |            "keyword": {
      |              "type": "keyword",
      |              "ignore_above": 256
      |            }
      |          }
      |        },
      |        "sessionId": {
      |          "type": "keyword"
      |        },
      |        "state": {
      |          "type": "keyword"
      |        },
      |        "statementId": {
      |          "type": "keyword"
      |        },
      |        "submitTime": {
      |          "type": "date",
      |          "format": "strict_date_time||epoch_millis"
      |        },
      |        "type": {
      |          "type": "keyword"
      |        }
      |   }
      | }
      |""".stripMargin
  val testTable = dataSourceName + ".default.flint_sql_test"

  // use a thread-local variable to store and manage the future in beforeEach and afterEach
  val threadLocalFuture = new ThreadLocal[Future[Unit]]()

  override def beforeAll(): Unit = {
    super.beforeAll()

    flintClient = new FlintOpenSearchClient(new FlintOptions(openSearchOptions.asJava));
    osClient = new OSClient(new FlintOptions(openSearchOptions.asJava))
    updater = new OpenSearchUpdater(
      requestIndex,
      new FlintOpenSearchClient(new FlintOptions(openSearchOptions.asJava)))

  }

  override def afterEach(): Unit = {
    flintClient.deleteIndex(requestIndex)
    super.afterEach()
  }

  def createSession(jobId: String, excludeJobId: String): Unit = {
    val docs = Seq(s"""{
        |  "state": "running",
        |  "lastUpdateTime": 1698796582978,
        |  "applicationId": "00fd777k3k3ls20p",
        |  "error": "",
        |  "sessionId": ${sessionId},
        |  "jobId": \"${jobId}\",
        |  "type": "session",
        |  "excludeJobIds": [\"${excludeJobId}\"]
        |}""".stripMargin)
    index(requestIndex, oneNodeSetting, requestIndexMapping, docs)
  }

  def startREPL(): Future[Unit] = {
    val prefix = "flint-repl-test"
    val threadPool = ThreadUtils.newDaemonThreadPoolScheduledExecutor(prefix, 1)
    implicit val executionContext = ExecutionContext.fromExecutor(threadPool)

    val futureResult = Future {
      // SparkConf's constructor creates a SparkConf that loads defaults from system properties and the classpath.
      // Read SparkConf.getSystemProperties
      System.setProperty(DATA_SOURCE_NAME.key, "my_glue1")
      System.setProperty(JOB_TYPE.key, "interactive")
      System.setProperty(SESSION_ID.key, sessionId)
      System.setProperty(REQUEST_INDEX.key, requestIndex)
      System.setProperty(EXCLUDE_JOB_IDS.key, "00fer5qo32fa080q")
      System.setProperty(REPL_INACTIVITY_TIMEOUT_MILLIS.key, "5000")
      System.setProperty(
        s"spark.sql.catalog.my_glue1",
        "org.opensearch.sql.FlintDelegatingSessionCatalog")
      System.setProperty("spark.master", "local")
      System.setProperty(HOST_ENDPOINT.key, openSearchHost)
      System.setProperty(HOST_PORT.key, String.valueOf(openSearchPort))
      System.setProperty(REFRESH_POLICY.key, "true")

      FlintREPL.envinromentProvider = new MockEnvironment(
        Map("SERVERLESS_EMR_JOB_ID" -> jobRunId, "SERVERLESS_EMR_VIRTUAL_CLUSTER_ID" -> appId))
      FlintREPL.enableHiveSupport = false
      FlintREPL.terminateJVM = false
      FlintREPL.main(Array(resultIndex))
    }
    futureResult.onComplete {
      case Success(result) => logInfo(s"Success result: $result")
      case Failure(ex) =>
        ex.printStackTrace()
        assert(false, s"An error has occurred: ${ex.getMessage}")
    }
    futureResult
  }

  def waitREPLStop(future: Future[Unit]): Unit = {
    try {
      ThreadUtils.awaitResult(future, Duration(1, MINUTES))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        assert(false, "failure waiting for REPL to finish")
    }
  }

  def submitQuery(query: String, queryId: String): String = {
    submitQuery(query, queryId, System.currentTimeMillis())
  }

  def submitQuery(query: String, queryId: String, submitTime: Long): String = {
    val statementId = UUID.randomUUID().toString

    updater.upsert(
      statementId,
      s"""{
         |  "sessionId": "${sessionId}",
         |  "query": "${query}",
         |  "applicationId": "00fd775baqpu4g0p",
         |  "state": "waiting",
         |  "submitTime": $submitTime,
         |  "type": "statement",
         |  "statementId": "${statementId}",
         |  "queryId": "${queryId}",
         |  "dataSourceName": "${dataSourceName}"
         |}""".stripMargin)
    statementId
  }

  test("sanity") {
    try {
      createSession(jobRunId, "")
      threadLocalFuture.set(startREPL())

      val createStatement =
        s"""
           | CREATE TABLE $testTable
           | (
           |   name STRING,
           |   age INT
           | )
           | USING CSV
           | OPTIONS (
           |  header 'false',
           |  delimiter '\\t'
           | )
           |""".stripMargin
      submitQuery(s"${makeJsonCompliant(createStatement)}", "99")

      val insertStatement =
        s"""
           | INSERT INTO $testTable
           | VALUES ('Hello', 30)
           | """.stripMargin
      submitQuery(s"${makeJsonCompliant(insertStatement)}", "100")

      val selectQueryId = "101"
      val selectQueryStartTime = System.currentTimeMillis()
      val selectQuery = s"SELECT name, age FROM $testTable".stripMargin
      val selectStatementId = submitQuery(s"${makeJsonCompliant(selectQuery)}", selectQueryId)

      val describeStatement = s"DESC $testTable".stripMargin
      val descQueryId = "102"
      val descStartTime = System.currentTimeMillis()
      val descStatementId = submitQuery(s"${makeJsonCompliant(describeStatement)}", descQueryId)

      val showTableStatement =
        s"SHOW TABLES IN " + dataSourceName + ".default LIKE 'flint_sql_test'"
      val showQueryId = "103"
      val showStartTime = System.currentTimeMillis()
      val showTableStatementId =
        submitQuery(s"${makeJsonCompliant(showTableStatement)}", showQueryId)

      val wrongSelectQueryId = "104"
      val wrongSelectQueryStartTime = System.currentTimeMillis()
      val wrongSelectQuery = s"SELECT name, age FROM testTable".stripMargin
      val wrongSelectStatementId =
        submitQuery(s"${makeJsonCompliant(wrongSelectQuery)}", wrongSelectQueryId)

      val lateSelectQueryId = "105"
      val lateSelectQuery = s"SELECT name, age FROM $testTable".stripMargin
      // submitted from last year. We won't pick it up
      val lateSelectStatementId =
        submitQuery(s"${makeJsonCompliant(selectQuery)}", selectQueryId, 1672101970000L)

      // clean up
      val dropStatement =
        s"""DROP TABLE $testTable""".stripMargin
      submitQuery(s"${makeJsonCompliant(dropStatement)}", "999")

      val selectQueryValidation: REPLResult => Boolean = result => {
        assert(
          result.results.size == 1,
          s"expected result size is 1, but got ${result.results.size}")
        val expectedResult = "{'name':'Hello','age':30}"
        assert(
          result.results(0).equals(expectedResult),
          s"expected result is $expectedResult, but got ${result.results(0)}")
        assert(
          result.schemas.size == 2,
          s"expected schema size is 2, but got ${result.schemas.size}")
        val expectedZerothSchema = "{'column_name':'name','data_type':'string'}"
        assert(
          result.schemas(0).equals(expectedZerothSchema),
          s"expected first field is $expectedZerothSchema, but got ${result.schemas(0)}")
        val expectedFirstSchema = "{'column_name':'age','data_type':'integer'}"
        assert(
          result.schemas(1).equals(expectedFirstSchema),
          s"expected second field is $expectedFirstSchema, but got ${result.schemas(1)}")
        commonValidation(result, selectQueryId, selectQuery, selectQueryStartTime)
        successValidation(result)
        true
      }
      pollForResultAndAssert(selectQueryValidation, selectQueryId)
      assert(
        !awaitConditionForStatementOrTimeout(
          statement => {
            statement.state == "success"
          },
          selectStatementId),
        s"Fail to verify for $selectStatementId.")

      val descValidation: REPLResult => Boolean = result => {
        assert(
          result.results.size == 2,
          s"expected result size is 2, but got ${result.results.size}")
        val expectedResult0 = "{'col_name':'name','data_type':'string'}"
        assert(
          result.results(0).equals(expectedResult0),
          s"expected result is $expectedResult0, but got ${result.results(0)}")
        val expectedResult1 = "{'col_name':'age','data_type':'int'}"
        assert(
          result.results(1).equals(expectedResult1),
          s"expected result is $expectedResult1, but got ${result.results(1)}")
        assert(
          result.schemas.size == 3,
          s"expected schema size is 3, but got ${result.schemas.size}")
        val expectedZerothSchema = "{'column_name':'col_name','data_type':'string'}"
        assert(
          result.schemas(0).equals(expectedZerothSchema),
          s"expected first field is $expectedZerothSchema, but got ${result.schemas(0)}")
        val expectedFirstSchema = "{'column_name':'data_type','data_type':'string'}"
        assert(
          result.schemas(1).equals(expectedFirstSchema),
          s"expected second field is $expectedFirstSchema, but got ${result.schemas(1)}")
        val expectedSecondSchema = "{'column_name':'comment','data_type':'string'}"
        assert(
          result.schemas(2).equals(expectedSecondSchema),
          s"expected third field is $expectedSecondSchema, but got ${result.schemas(2)}")
        commonValidation(result, descQueryId, describeStatement, descStartTime)
        successValidation(result)
        true
      }
      pollForResultAndAssert(descValidation, descQueryId)
      assert(
        !awaitConditionForStatementOrTimeout(
          statement => {
            statement.state == "success"
          },
          descStatementId),
        s"Fail to verify for $descStatementId.")

      val showValidation: REPLResult => Boolean = result => {
        assert(
          result.results.size == 1,
          s"expected result size is 1, but got ${result.results.size}")
        val expectedResult =
          "{'namespace':'default','tableName':'flint_sql_test','isTemporary':false}"
        assert(
          result.results(0).equals(expectedResult),
          s"expected result is $expectedResult, but got ${result.results(0)}")
        assert(
          result.schemas.size == 3,
          s"expected schema size is 3, but got ${result.schemas.size}")
        val expectedZerothSchema = "{'column_name':'namespace','data_type':'string'}"
        assert(
          result.schemas(0).equals(expectedZerothSchema),
          s"expected first field is $expectedZerothSchema, but got ${result.schemas(0)}")
        val expectedFirstSchema = "{'column_name':'tableName','data_type':'string'}"
        assert(
          result.schemas(1).equals(expectedFirstSchema),
          s"expected second field is $expectedFirstSchema, but got ${result.schemas(1)}")
        val expectedSecondSchema = "{'column_name':'isTemporary','data_type':'boolean'}"
        assert(
          result.schemas(2).equals(expectedSecondSchema),
          s"expected third field is $expectedSecondSchema, but got ${result.schemas(2)}")
        commonValidation(result, showQueryId, showTableStatement, showStartTime)
        successValidation(result)
        true
      }
      pollForResultAndAssert(showValidation, showQueryId)
      assert(
        !awaitConditionForStatementOrTimeout(
          statement => {
            statement.state == "success"
          },
          showTableStatementId),
        s"Fail to verify for $showTableStatementId.")

      val wrongSelectQueryValidation: REPLResult => Boolean = result => {
        assert(
          result.results.size == 0,
          s"expected result size is 0, but got ${result.results.size}")
        assert(
          result.schemas.size == 0,
          s"expected schema size is 0, but got ${result.schemas.size}")
        commonValidation(result, wrongSelectQueryId, wrongSelectQuery, wrongSelectQueryStartTime)
        failureValidation(result)
        true
      }
      pollForResultAndAssert(wrongSelectQueryValidation, wrongSelectQueryId)
      assert(
        !awaitConditionForStatementOrTimeout(
          statement => {
            statement.state == "failed"
          },
          wrongSelectStatementId),
        s"Fail to verify for $wrongSelectStatementId.")

      // expect time out as this statement should not be picked up
      assert(
        awaitConditionForStatementOrTimeout(
          statement => {
            statement.state != "waiting"
          },
          lateSelectStatementId),
        s"Fail to verify for $lateSelectStatementId.")
    } catch {
      case e: Exception =>
        logError("Unexpected exception", e)
        assert(false, "Unexpected exception")
    } finally {
      waitREPLStop(threadLocalFuture.get())
      threadLocalFuture.remove()

      // shutdown hook is called after all tests have finished. We cannot verify if session has correctly been set in IT.
    }
  }

  /**
   * JSON does not support raw newlines (\n) in string values. All newlines must be escaped or
   * removed when inside a JSON string. The same goes for tab characters, which should be
   * represented as \\t.
   *
   * Here, I replace the newlines with spaces and escape tab characters that is being included in
   * the JSON.
   *
   * @param sqlQuery
   * @return
   */
  def makeJsonCompliant(sqlQuery: String): String = {
    sqlQuery.replaceAll("\n", " ").replaceAll("\t", "\\\\t")
  }

  def commonValidation(
      result: REPLResult,
      expectedQueryId: String,
      expectedStatement: String,
      queryStartTime: Long): Unit = {
    assert(
      result.jobRunId.equals(jobRunId),
      s"expected job id is $jobRunId, but got ${result.jobRunId}")
    assert(
      result.applicationId.equals(appId),
      s"expected app id is $appId, but got ${result.applicationId}")
    assert(
      result.dataSourceName.equals(dataSourceName),
      s"expected data source is $dataSourceName, but got ${result.dataSourceName}")
    assert(
      result.queryId.equals(expectedQueryId),
      s"expected query id is $expectedQueryId, but got ${result.queryId}")
    assert(
      result.queryText.equals(expectedStatement),
      s"expected query is $expectedStatement, but got ${result.queryText}")
    assert(
      result.sessionId.equals(sessionId),
      s"expected session id is $sessionId, but got ${result.sessionId}")
    assert(
      result.updateTime > queryStartTime,
      s"expect that update time is ${result.updateTime} later than query start time $queryStartTime, but it is not")
    assert(
      result.queryRunTime > 0,
      s"expected query run time is positive, but got ${result.queryRunTime}")
    assert(
      result.queryRunTime < System.currentTimeMillis() - queryStartTime,
      s"expected query run time ${result.queryRunTime} should be less than ${System
          .currentTimeMillis() - queryStartTime}, but it is not")
  }

  def successValidation(result: REPLResult): Unit = {
    assert(
      result.status.equals("SUCCESS"),
      s"expected status is SUCCESS, but got ${result.status}")
    assert(result.error.isEmpty, s"we don't expect error, but got ${result.error}")
  }

  def failureValidation(result: REPLResult): Unit = {
    assert(result.status.equals("FAILED"), s"expected status is FAILED, but got ${result.status}")
    assert(!result.error.isEmpty, s"we expect error, but got nothing")
  }

  def pollForResultAndAssert(expected: REPLResult => Boolean, queryId: String): Unit = {
    pollForResultAndAssert(osClient, expected, "queryId", queryId, 60000, resultIndex)
  }

  /**
   * Repeatedly polls a resource until a specified condition is met or a timeout occurs.
   *
   * This method continuously checks a resource for a specific condition. If the condition is met
   * within the timeout period, the polling stops. If the timeout period is exceeded without the
   * condition being met, an assertion error is thrown.
   *
   * @param osClient
   *   The OSClient used to poll the resource.
   * @param condition
   *   A function that takes an instance of type T and returns a Boolean. This function defines
   *   the condition to be met.
   * @param id
   *   The unique identifier of the resource to be polled.
   * @param timeoutMillis
   *   The maximum amount of time (in milliseconds) to wait for the condition to be met.
   * @param index
   *   The index in which the resource resides.
   * @param deserialize
   *   A function that deserializes a String into an instance of type T.
   * @param logType
   *   A descriptive string for logging purposes, indicating the type of resource being polled.
   * @return
   *   whether timeout happened
   * @throws OpenSearchStatusException
   *   if there's an issue fetching the resource.
   */
  def awaitConditionOrTimeout[T](
      osClient: OSClient,
      expected: T => Boolean,
      id: String,
      timeoutMillis: Long,
      index: String,
      deserialize: String => T,
      logType: String): Boolean = {
    val getResponse = osClient.getDoc(index, id)
    val startTime = System.currentTimeMillis()
    breakable {
      while (System.currentTimeMillis() - startTime < timeoutMillis) {
        logInfo(s"Check $logType for $id")
        try {
          if (getResponse.isExists()) {
            val instance = deserialize(getResponse.getSourceAsString)
            logInfo(s"$logType $id: $instance")
            if (expected(instance)) {
              break
            }
          }
        } catch {
          case e: OpenSearchStatusException => logError(s"Exception while fetching $logType", e)
        }
        Thread.sleep(2000) // 2 seconds
      }
    }
    System.currentTimeMillis() - startTime >= timeoutMillis
  }

  def awaitConditionForStatementOrTimeout(
      expected: FlintCommand => Boolean,
      statementId: String): Boolean = {
    awaitConditionOrTimeout[FlintCommand](
      osClient,
      expected,
      statementId,
      10000,
      requestIndex,
      FlintCommand.deserialize,
      "statement")
  }

  def awaitConditionForSessionOrTimeout(
      expected: FlintInstance => Boolean,
      sessionId: String): Boolean = {
    awaitConditionOrTimeout[FlintInstance](
      osClient,
      expected,
      sessionId,
      10000,
      requestIndex,
      FlintInstance.deserialize,
      "session")
  }
}
