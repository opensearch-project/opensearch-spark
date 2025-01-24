/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.spark.e2e

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import scala.collection.mutable.ListBuffer
import scala.io.Source.fromFile

import org.scalatest.{Assertions, BeforeAndAfterAll, Suite}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks
import play.api.libs.json.{JsError, Json, JsValue}
import sttp.client3.{basicRequest, HttpClientSyncBackend, Identity, Response, ResponseException, SttpBackend, UriContext}
import sttp.client3.playJson.asJson

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}

/**
 * Tests requiring the  should extend OpenSearchSuite.
 */
class EndToEndITSuite extends AnyFlatSpec with TableDrivenPropertyChecks with BeforeAndAfterAll with SparkTrait with S3ClientTrait with Assertions with Logging {
  self: Suite =>

  val DOCKER_INTEG_DIR: String = "docker/integ-test"
  val DOCKER_VOLUMES: Array[String] = Array("integ-test_metastore-data", "integ-test_minio-data", "integ-test_opensearch-data")
  var OPENSEARCH_URL: String = null
  val OPENSEARCH_USERNAME: String = "admin"
  var OPENSEARCH_PASSWORD: String = null
  var SPARK_CONNECT_PORT: Int = 0
  var S3_ACCESS_KEY: String = null
  var S3_SECRET_KEY: String = null
  val INTEG_TEST_BUCKET: String = "integ-test"
  val TEST_RESOURCES_BUCKET: String = "test-resources"
  val S3_DATASOURCE: String = "mys3"

  override def getSparkConnectPort(): Int = {
    SPARK_CONNECT_PORT
  }

  override def getS3AccessKey(): String = {
    S3_ACCESS_KEY
  }

  override def getS3SecretKey(): String = {
    S3_SECRET_KEY
  }

  override def beforeAll(): Unit = {
    logInfo("Starting docker cluster")

    val dockerEnv = new Properties()
    dockerEnv.load(new FileInputStream(new File(DOCKER_INTEG_DIR, ".env")))
    OPENSEARCH_URL = "http://localhost:" + dockerEnv.getProperty("OPENSEARCH_PORT")
    OPENSEARCH_PASSWORD = dockerEnv.getProperty("OPENSEARCH_ADMIN_PASSWORD")
    SPARK_CONNECT_PORT = Integer.parseInt(dockerEnv.getProperty("SPARK_CONNECT_PORT"))
    S3_ACCESS_KEY = dockerEnv.getProperty("S3_ACCESS_KEY")
    S3_SECRET_KEY = dockerEnv.getProperty("S3_SECRET_KEY")

    val cmdWithArgs = List("docker", "volume", "rm") ++ DOCKER_VOLUMES
    val deleteDockerVolumesProcess = new ProcessBuilder(cmdWithArgs.toArray: _*).start()
    deleteDockerVolumesProcess.waitFor(10, TimeUnit.SECONDS)

    val dockerProcess = new ProcessBuilder("docker", "compose", "up", "-d")
      .directory(new File(DOCKER_INTEG_DIR))
      .start()
    dockerProcess.waitFor(5, TimeUnit.MINUTES)

    if (dockerProcess.exitValue() != 0) {
      logError("Unable to start docker cluster")
    }

    logInfo("Started docker cluster")

    createTables()
    createIndices()
  }

  override def afterAll(): Unit = {
    logInfo("Stopping docker cluster")
    waitForSparkSubmitCompletion()

    val dockerProcess = new ProcessBuilder("docker", "compose", "down")
      .directory(new File(DOCKER_INTEG_DIR))
      .start()
    dockerProcess.waitFor(2, TimeUnit.MINUTES)

    if (dockerProcess.exitValue() != 0) {
      logError("Unable to stop docker cluster")
    }

    logInfo("Stopped docker cluster")
  }

  def waitForSparkSubmitCompletion(): Unit = {
    val endTime = System.currentTimeMillis() + 300000
    while (System.currentTimeMillis() < endTime) {
      val dockerProcess = new ProcessBuilder("docker", "ps").start()
      val outputReader = new BufferedReader(new InputStreamReader(dockerProcess.getInputStream))

      // Ignore the header
      outputReader.readLine()
      var line = outputReader.readLine()
      val pattern = Pattern.compile("^[^ ]+ +integ-test-spark-submit:latest +.*")
      var matched = false
      while (line != null) {
        if (pattern.matcher(line).matches()) {
          matched = true
        }
        line = outputReader.readLine()
      }

      if (matched) {
        outputReader.close()
        dockerProcess.waitFor(2, TimeUnit.SECONDS)
        Thread.sleep(5000)
      } else {
        return
      }
    }
  }

  def createTables(): Unit = {
    try {
      val tablesDir = new File("e2e-test/src/test/resources/spark/tables")
      tablesDir.listFiles((_, name) => name.endsWith(".parquet")).foreach(f => {
        val tableName = f.getName.substring(0, f.getName.length() - 8)
        getS3Client().putObject(TEST_RESOURCES_BUCKET, "spark/tables/" + f.getName, f)

        try {
          val df = getSparkSession().read.parquet(s"s3a://$TEST_RESOURCES_BUCKET/spark/tables/" + f.getName)
          df.write.option("path", s"s3a://$INTEG_TEST_BUCKET/$tableName").saveAsTable(tableName)
        } catch {
          case e: Exception => logError("Unable to create table", e)
        }
      })
    } catch {
      case e: Exception => logError("Failure", e)
    }
  }

  def createIndices(): Unit = {
    val indicesDir = new File("e2e-test/src/test/resources/opensearch/indices")
    val backend = HttpClientSyncBackend()

    indicesDir.listFiles((_, name) => name.endsWith(".mapping.json")).foreach(f => {
      val indexName = f.getName.substring(0, f.getName.length() - 13)

      val checkIndexRequest = basicRequest.get(uri"$OPENSEARCH_URL/$indexName")
        .auth.basic(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
      val response = checkIndexRequest.send(backend)
      if (response.isSuccess) {
        val deleteIndexRequest = basicRequest.delete(uri"$OPENSEARCH_URL/$indexName")
          .auth.basic(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
        deleteIndexRequest.send(backend)
      }

      val createIndexRequest = basicRequest.put(uri"$OPENSEARCH_URL/$indexName")
        .auth.basic(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
        .contentType("application/json")
        .body(new FileInputStream(f))
      createIndexRequest.send(backend)

      val dataFile = new File(f.getParent, indexName + ".json")
      if (dataFile.exists()) {
        val bulkInsertRequest = basicRequest.post(uri"$OPENSEARCH_URL/$indexName/_bulk")
          .auth.basic(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
          .contentType("application/x-ndjson")
          .body(new FileInputStream(new File(f.getParent, indexName + ".json")))
        bulkInsertRequest.send(backend)
      }
    })
  }

  it should "SQL Queries" in {
    val queriesDir = new File("e2e-test/src/test/resources/spark/queries/sql")
    val queriesTableData : ListBuffer[(String, String)] = new ListBuffer()

    queriesDir.listFiles((_, name) => name.endsWith(".sql")).foreach(f => {
      val querySource = fromFile(f)
      val query = querySource.mkString
      querySource.close()

      val baseName = f.getName.substring(0, f.getName.length - 4)
      queriesTableData += ((query, baseName))
    })

    forEvery(Table(("Query", "Base Filename"), queriesTableData: _*)) { (query, baseName) =>
      logInfo(s">>> Testing query [$baseName]: $query")
      val results : Dataset[Row] = getSparkSession().sql(query).coalesce(1)

      val s3Folder = s"spark/query-results/sql/$baseName"
      results.write.format("csv").option("header", "true").save(s"s3a://$TEST_RESOURCES_BUCKET/$s3Folder")

      val actualResults = getActualResults(s"$s3Folder/")
      val expectedFile = new File(s"e2e-test/src/test/resources/spark/queries/sql/$baseName.results")
      val expectedSource = fromFile(expectedFile)
      val expectedResults = expectedSource.mkString.stripTrailing()
      expectedSource.close()

      assert(expectedResults == actualResults)
    }
  }

  it should "PPL Queries" in {
    val queriesDir = new File("e2e-test/src/test/resources/spark/queries/ppl")
    val queriesTableData : ListBuffer[(String, String)] = new ListBuffer()

    queriesDir.listFiles((_, name) => name.endsWith(".ppl")).foreach(f => {
      val querySource = fromFile(f)
      val query = querySource.mkString
      querySource.close()

      val baseName = f.getName.substring(0, f.getName.length - 4)
      queriesTableData += ((query, baseName))
    })

    forEvery(Table(("Query", "Base Filename"), queriesTableData: _*)) { (query, baseName) =>
      logInfo(s">>> Testing query [$baseName]: $query")
      val results : Dataset[Row] = getSparkSession().sql(query).coalesce(1)

      val s3Folder = s"spark/query-results/ppl/$baseName"
      results.write.format("csv").option("header", "true").save(s"s3a://$TEST_RESOURCES_BUCKET/$s3Folder")

      val actualResults = getActualResults(s"$s3Folder/")
      val expectedFile = new File(s"e2e-test/src/test/resources/spark/queries/ppl/$baseName.results")
      val expectedSource = fromFile(expectedFile)
      val expectedResults = expectedSource.mkString.stripTrailing()
      expectedSource.close()

      assert(expectedResults == actualResults)
    }
  }

  it should "Async SQL Queries" in {
    var sessionId : String = null
    val backend = HttpClientSyncBackend()

    val queriesDir = new File("e2e-test/src/test/resources/opensearch/queries/sql")
    val queriesTableData : ListBuffer[(String, String)] = new ListBuffer()

    queriesDir.listFiles((_, name) => name.endsWith(".sql")).foreach(f => {
      val querySource = fromFile(f)
      val query = querySource.mkString
      querySource.close()

      val baseName = f.getName.substring(0, f.getName.length - 4)
      queriesTableData += ((query, baseName))
    })

    forEvery(Table(("Query", "Base Filename"), queriesTableData: _*)) { (query: String, baseName: String) =>
      logInfo(s">>> Testing query [$baseName]: $query")
      val queryResponse = executeAsyncQuery("sql", query, sessionId, backend)

      if (queryResponse.isSuccess) {
        val responseData = queryResponse.body.right.get
        val queryId = (responseData \ "queryId").as[String]
        if (sessionId == null) {
          sessionId = (responseData \ "sessionId").as[String]
        }

        val actualResults = getAsyncResults(queryId, backend)
        val expectedResults = Json.parse(new FileInputStream(new File(queriesDir, baseName + ".results")))

        assert(expectedResults == actualResults)
      }
    }
  }

  it should "Async PPL Queries" in {
    var sessionId : String = null
    val backend = HttpClientSyncBackend()

    val queriesDir = new File("e2e-test/src/test/resources/opensearch/queries/ppl")
    val queriesTableData : ListBuffer[(String, String)] = new ListBuffer()

    queriesDir.listFiles((_, name) => name.endsWith(".ppl")).foreach(f => {
      val querySource = fromFile(f)
      val query = querySource.mkString
      querySource.close()

      val baseName = f.getName.substring(0, f.getName.length - 4)
      queriesTableData += ((query, baseName))
    })

    forEvery(Table(("Query", "Base Filename"), queriesTableData: _*)) { (query: String, baseName: String) =>
      logInfo(s">>> Testing query [$baseName]: $query")
      val queryResponse = executeAsyncQuery("ppl", query, sessionId, backend)

      if (queryResponse.isSuccess) {
        val responseData = queryResponse.body.right.get
        val queryId = (responseData \ "queryId").as[String]
        if (sessionId == null) {
          sessionId = (responseData \ "sessionId").as[String]
        }

        val actualResults = getAsyncResults(queryId, backend)
        val expectedResults = Json.parse(new FileInputStream(new File(queriesDir, baseName + ".results")))

        assert(expectedResults == actualResults)
      }
    }
  }

  def getActualResults(s3Path : String): String = {
    val objectSummaries = getS3Client().listObjects("test-resources", s3Path).getObjectSummaries
    var jsonKey : String = null
    for (i <- 0 until objectSummaries.size()) {
      val objectSummary = objectSummaries.get(i)
      if (jsonKey == null && objectSummary.getKey.endsWith(".csv")) {
        jsonKey = objectSummary.getKey
      }
    }

    val results = new ListBuffer[String]
    if (jsonKey != null) {
      val s3Object = getS3Client().getObject("test-resources", jsonKey)
      val reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent))

      var line = reader.readLine()
      while (line != null) {
        results += line
        line = reader.readLine()
      }
      reader.close()

      return results.mkString("\n").stripTrailing()
    }

    throw new Exception("Object not found")
  }

  def executeAsyncQuery(language: String, query: String, sessionId: String, backend: SttpBackend[Identity, Any]) : Identity[Response[Either[ResponseException[String, JsError], JsValue]]] = {
    var queryBody : String = null
    if (sessionId == null) {
      queryBody = "{\"datasource\": \"" + S3_DATASOURCE + "\", \"lang\": \"" + language + "\", \"query\": \"" + query + "\"}"
    } else {
      queryBody = "{\"datasource\": \"" + S3_DATASOURCE + "\", \"lang\": \"" + language + "\", \"query\": \"" + query + "\", \"sessionId\": \"" + sessionId + "\"}"
    }
    basicRequest.post(uri"$OPENSEARCH_URL/_plugins/_async_query")
      .auth.basic(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
      .contentType("application/json")
      .body(queryBody.getBytes)
      .response(asJson[JsValue])
      .send(backend)
  }

  def getAsyncResults(queryId: String, backend: SttpBackend[Identity, Any]): JsValue = {
    val endTime = System.currentTimeMillis() + 30000

    while (System.currentTimeMillis() < endTime) {
      val response = basicRequest.get(uri"$OPENSEARCH_URL/_plugins/_async_query/$queryId")
        .auth.basic(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
        .contentType("application/json")
        .response(asJson[JsValue])
        .send(backend)

      if (response.isSuccess) {
        val responseBody = response.body.right.get
        val status = (responseBody \ "status").asOpt[String]

        if (status.isDefined) {
          if (status.get == "SUCCESS") {
            return responseBody
          }
        }
      }

      Thread.sleep(500)
    }

    throw new IllegalStateException("Unable to get async query results")
  }
}
