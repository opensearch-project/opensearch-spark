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
 * Tests queries for expected results on the integration test docker cluster. Queries can be run using
 * Spark Connect or the OpenSearch Async Query API.
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

  /**
   * Starts up the integration test docker cluster.
   */
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
    var stopReading = false
    new Thread() {
      override def run(): Unit = {
        val reader = new BufferedReader(new InputStreamReader(dockerProcess.getInputStream))
        var line = reader.readLine()
        while (!stopReading && line != null) {
          logInfo("*** " + line)
          line = reader.readLine()
        }
      }
    }.start()
    val completed = dockerProcess.waitFor(20, TimeUnit.MINUTES)
    stopReading = true
    if (!completed) {
      throw new IllegalStateException("Unable to start docker cluster")
    }

    if (dockerProcess.exitValue() != 0) {
      logError("Unable to start docker cluster")
    }

    logInfo("Started docker cluster")

    // Wait for services to be ready
    waitForService("localhost", 9000)  // MinIO
    waitForService("localhost", SPARK_CONNECT_PORT)  // Spark
    waitForService("localhost", dockerEnv.getProperty("OPENSEARCH_PORT").toInt)  // OpenSearch

    // Wait for S3 service to be available before proceeding
    if (!waitForS3Available()) {
      throw new IllegalStateException("S3 service did not become available")
    }

    // Add these lines to verify and create required buckets
    try {
      ensureBucketExists(INTEG_TEST_BUCKET)      // "XXXXXXXXXX" bucket
      logInfo(s"Successfully verified/created bucket '$INTEG_TEST_BUCKET'")
      ensureBucketExists(TEST_RESOURCES_BUCKET)   // "XXXXXXXXXXXXXX" bucket
      logInfo(s"Successfully verified/created bucket '$TEST_RESOURCES_BUCKET'")
      // Ensure the datasource exists for Async Query API
      if (!ensureDataSourceExists()) {
        logError("Failed to create or verify datasource for Async Query API")
      }
    } catch {
      case e: Exception =>
        logError(s"Failed to ensure buckets exist: ${e.getMessage}", e)
        throw e
    }
    createTables()
    createIndices()
  }

  /**
   * Shuts down the integration test docker cluster.
   */
  override def afterAll(): Unit = {
    logInfo("Stopping docker cluster")
    waitForSparkSubmitCompletion()

    val dockerProcess = new ProcessBuilder("docker", "compose", "down")
      .directory(new File(DOCKER_INTEG_DIR))
      .start()
    dockerProcess.waitFor(10, TimeUnit.MINUTES)

    if (dockerProcess.exitValue() != 0) {
      logError("Unable to stop docker cluster")
    }

    logInfo("Stopped docker cluster")
  }

  /**
   * Wait for all Spark submit containers to finish. Spark submit containers are used for processing Async Query
   * API requests. Each Async Query API session will have at most one Spark submit container.
   */
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

  /**
   * Creates Spark tables. Looks for parquet files in "e2e-test/src/test/resources/spark/tables".
   *
   * The tables are created as external tables with the data stored in the MinIO(S3) container.
   */
  def createTables(): Unit = {
    def withRetry[T](operation: => T, maxAttempts: Int = 5, initialWait: Long = 2000): T = {
      def retry(remainingAttempts: Int, wait: Long): T = {
        try {
          operation
        } catch {
          case e: Exception if remainingAttempts > 1 =>
            logInfo(s"Operation failed, retrying in ${wait}ms. ${remainingAttempts - 1} attempts remaining")
            Thread.sleep(wait)
            retry(remainingAttempts - 1, wait * 2)
          case e: Exception =>
            logError("Operation failed after all retry attempts", e)
            throw e
        }
      }
      retry(maxAttempts, initialWait)
    }
    try {
      val tablesDir = new File("e2e-test/src/test/resources/spark/tables")
      tablesDir.listFiles((_, name) => name.endsWith(".parquet")).foreach(f => {
        val tableName = f.getName.substring(0, f.getName.length() - 8)
        withRetry {
          getS3Client().putObject(TEST_RESOURCES_BUCKET, "spark/tables/" + f.getName, f)
          try {
            // We need to use S3 since the local file system is not accessible from the Docker container
            // First upload the file to S3 (this is already done above with getS3Client().putObject)
            // Then create an external table pointing to the S3 location
            getSparkSession().sql(s"DROP TABLE IF EXISTS $tableName")
            // Create an external table pointing to the S3 location where the parquet file was uploaded
            getSparkSession().sql(
              s"""
                 |CREATE EXTERNAL TABLE $tableName
                 |USING parquet
                 |LOCATION 's3a://$TEST_RESOURCES_BUCKET/spark/tables/${f.getName}'
                 |""".stripMargin)
          } catch {
            case e: Exception =>
              logError("Unable to create table", e)
              throw e
          }
        }
      })
    } catch {
      case e: Exception => logError("Failure", e)
      throw e
    }
  }

  /**
  * Waits for a network service to become available at the specified host and port.
  * This method attempts to establish a socket connection repeatedly until successful
  * or until the timeout is reached.
  *
  * @param host      The hostname or IP address of the service to connect to
  * @param port      The port number of the service
  * @param timeoutMs The maximum time to wait in milliseconds before giving up (defaults to 60000ms/60 seconds)
  * @throws IllegalStateException if the service is not available within the specified timeout period
  */
  def waitForService(host: String, port: Int, timeoutMs: Long = 60000): Unit = {
    val endTime = System.currentTimeMillis() + timeoutMs
    var connected = false
    while (!connected && System.currentTimeMillis() < endTime) {
      try {
        val socket = new java.net.Socket()
        socket.connect(new java.net.InetSocketAddress(host, port), 1000)
        socket.close()
        connected = true
      } catch {
        case _: Exception =>
          Thread.sleep(2000)
      }
    }
    if (!connected) {
      throw new IllegalStateException(s"Service $host:$port not available after ${timeoutMs}ms")
    }
  }

  /**
   * Creates OpenSearch indices. Looks for ".mapping.json" files in
   * "e2e-test/src/test/resources/opensearch/indices".
   *
   * An index is created using the mapping data in the ".mapping.json" file. If there is a similarly named
   * file with only a ".json" extension, then it is used to do a bulk import of data into the index.
   */
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

  /**
  * Attempts to verify S3 service availability by trying to list buckets with exponential backoff.
  *
  * This method makes multiple attempts to connect to S3, implementing an exponential backoff
  * strategy between attempts. It will try up to 10 times, with increasing delay between
  * each attempt.
  *
  * @return true if S3 service becomes available within the maximum number of attempts,
  *         false if the service remains unavailable after all attempts
  */
  def waitForS3Available(): Boolean = {
    val maxRetries = 10
    val initialBackoffMs = 1000
    for (attempt <- 1 to maxRetries) {
      try {
        // Try a simple operation like listing buckets
        getS3Client().listBuckets()
        logInfo(s"S3 service available after $attempt attempts")
        return true
      } catch {
        case e: Exception =>
          val backoffTime = initialBackoffMs * Math.pow(2, attempt - 1).toInt
          logInfo(s"S3 service not available yet (attempt $attempt/$maxRetries), waiting ${backoffTime}ms: ${e.getMessage}")
          Thread.sleep(backoffTime)
      }
    }

    logError(s"S3 service not available after $maxRetries attempts")
    false
  }

  /**
   * Creates or updates the S3 datasource in OpenSearch for use with the Async Query API.
   * This method ensures that the "mys3" datasource exists and is properly configured to
   * use the MinIO service.
   *
   * @return true if the datasource was created or already exists, false otherwise
   */
  def ensureDataSourceExists(): Boolean = {
    val backend = HttpClientSyncBackend()
    val maxRetries = 5
    val initialBackoffMs = 1000
    for (attempt <- 1 to maxRetries) {
      try {
        // Check if the datasource exists
        logInfo(s"Checking if datasource $S3_DATASOURCE exists (attempt $attempt/$maxRetries)")
        val checkResponse = basicRequest.get(uri"$OPENSEARCH_URL/_plugins/_query/datasource/$S3_DATASOURCE")
          .auth.basic(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
          .send(backend)
        if (checkResponse.code.code == 404) {
          logInfo(s"Datasource $S3_DATASOURCE does not exist, creating it")
          // Create the datasource
          val createDatasourceBody = s"""{
            "connector": "s3glue",
            "properties": {
              "glue.auth.type": "access_key",
              "glue.auth.access_key": "$S3_ACCESS_KEY",
              "glue.auth.secret_key": "$S3_SECRET_KEY",
              "s3.endpoint": "http://minio-S3:9000",
              "s3.path.style.access": "true"
            }
          }"""
          val createResponse = basicRequest.put(uri"$OPENSEARCH_URL/_plugins/_query/datasource/$S3_DATASOURCE")
            .auth.basic(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
            .contentType("application/json")
            .body(createDatasourceBody)
            .send(backend)
          if (createResponse.isSuccess) {
            logInfo(s"Successfully created datasource $S3_DATASOURCE")
            return true
          } else {
            logError(s"Failed to create datasource $S3_DATASOURCE: ${createResponse.body}")
            // Try an alternative approach using cluster settings
            logInfo("Trying alternative approach using cluster settings")
            val clusterSettingsBody = s"""{
              "persistent": {
                "plugins.query.datasource.$S3_DATASOURCE.auth.type": "access_key",
                "plugins.query.datasource.$S3_DATASOURCE.auth.access_key": "$S3_ACCESS_KEY",
                "plugins.query.datasource.$S3_DATASOURCE.auth.secret_key": "$S3_SECRET_KEY",
                "plugins.query.datasource.$S3_DATASOURCE.endpoint": "http://minio-S3:9000",
                "plugins.query.datasource.$S3_DATASOURCE.path_style_access": "true"
              }
            }"""
            val settingsResponse = basicRequest.put(uri"$OPENSEARCH_URL/_cluster/settings")
              .auth.basic(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
              .contentType("application/json")
              .body(clusterSettingsBody)
              .send(backend)
            if (settingsResponse.isSuccess) {
              logInfo(s"Successfully configured datasource $S3_DATASOURCE using cluster settings")
              return true
            } else {
              logError(s"Failed to configure datasource using cluster settings: ${settingsResponse.body}")
              // If we're not on the last attempt, wait and retry
              if (attempt < maxRetries) {
                val backoffTime = initialBackoffMs * Math.pow(2, attempt - 1).toInt
                logInfo(s"Retrying in ${backoffTime}ms")
                Thread.sleep(backoffTime)
              }
            }
          }
        } else if (checkResponse.isSuccess) {
          logInfo(s"Datasource $S3_DATASOURCE already exists")
          return true
        } else {
          logError(s"Error checking if datasource exists: ${checkResponse.body}")
          // If we're not on the last attempt, wait and retry
          if (attempt < maxRetries) {
            val backoffTime = initialBackoffMs * Math.pow(2, attempt - 1).toInt
            logInfo(s"Retrying in ${backoffTime}ms")
            Thread.sleep(backoffTime)
          }
        }
      } catch {
        case e: Exception =>
          logError(s"Exception while ensuring datasource exists: ${e.getMessage}", e)
          // If we're not on the last attempt, wait and retry
          if (attempt < maxRetries) {
            val backoffTime = initialBackoffMs * Math.pow(2, attempt - 1).toInt
            logInfo(s"Retrying in ${backoffTime}ms")
            Thread.sleep(backoffTime)
          }
      }
    }
    logError(s"Failed to ensure datasource $S3_DATASOURCE exists after $maxRetries attempts")
    false
  }
  /**
   * Tests SQL queries on the "spark" container. Looks for ".sql" files in
   * "e2e-test/src/test/resources/spark/queries/sql".
   *
   * Uses Spark Connect to run the query on the "spark" container and compares the results to the expected results
   * in the corresponding ".results" file. The ".results" file is in CSV format with a header.
   */
  it should "SQL Queries" ignore {
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

  /**
   * Tests PPL queries on the "spark" container. Looks for ".ppl" files in
   * "e2e-test/src/test/resources/spark/queries/ppl".
   *
   * Uses Spark Connect to run the query on the "spark" container and compares the results to the expected results
   * in the corresponding ".results" file. The ".results" file is in CSV format with a header.
   */
  it should "PPL Queries" ignore {
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

  /**
   * Tests SQL queries using the Async Query API. Looks for ".sql" files in
   * "e2e-test/src/test/resources/opensearch/queries/sql".
   *
   * Submits the query using a REST call to the Async Query API. Will wait until the results are available and
   * then compare them to the expected results in the corresponding ".results" file. The ".results" file is the
   * JSON response from fetching the results.
   *
   * All queries are tested using the same Async Query API session.
   */
  it should "Async SQL Queries" ignore {
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
      try {
        val queryResponse = executeAsyncQuery("sql", query, sessionId, backend)

        if (queryResponse.isSuccess) {
          val responseData = queryResponse.body.right.get
          logInfo(s"Response data: ${responseData}")
          val queryId = (responseData \ "queryId").as[String]
          if (sessionId == null) {
            sessionId = (responseData \ "sessionId").as[String]
          }

          val actualResults = getAsyncResults(queryId, backend)
          val expectedResults = Json.parse(new FileInputStream(new File(queriesDir, baseName + ".results")))

          assert(expectedResults == actualResults)
        } else {
          logError(s"Query response failed: ${queryResponse.body}")
          fail(s"Query response failed: ${queryResponse.body}")
        }
      } catch {
        case e: Exception =>
          logError(s"Exception during async SQL query execution: ${e.getMessage}", e)
          fail(s"Exception during async SQL query execution: ${e.getMessage}")
      }
    }
  }

  /**
   * Tests SQL queries using the Async Query API. Looks for ".ppl" files in
   * "e2e-test/src/test/resources/opensearch/queries/ppl".
   *
   * Submits the query using a REST call to the Async Query API. Will wait until the results are available and
   * then compare them to the expected results in the corresponding ".results" file. The ".results" file is the
   * JSON response from fetching the results.
   *
   * All queries are tested using the same Async Query API session.
   */
  it should "Async PPL Queries" ignore {
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
      try {
        val queryResponse = executeAsyncQuery("ppl", query, sessionId, backend)

        if (queryResponse.isSuccess) {
          val responseData = queryResponse.body.right.get
          logInfo(s"Response data: ${responseData}")
          val queryId = (responseData \ "queryId").as[String]
          if (sessionId == null) {
            sessionId = (responseData \ "sessionId").as[String]
          }

          val actualResults = getAsyncResults(queryId, backend)
          val expectedResults = Json.parse(new FileInputStream(new File(queriesDir, baseName + ".results")))

          assert(expectedResults == actualResults)
        } else {
          logError(s"Query response failed: ${queryResponse.body}")
          fail(s"Query response failed: ${queryResponse.body}")
        }
      } catch {
        case e: Exception =>
          logError(s"Exception during async PPL query execution: ${e.getMessage}", e)
          fail(s"Exception during async PPL query execution: ${e.getMessage}")
      }
    }
  }

  /**
   * Retrieves the results from S3 of a query submitted using Spark Connect. The results are saved in S3 in CSV
   * format.
   *
   * @param s3Path S3 "folder" where the results were saved
   * @return CSV formatted results
   */
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

  /**
   * Submits a request to the Async Query API to execute a query.
   *
   * @param language query language (either "ppl" or "sql")
   * @param query query to execute
   * @param sessionId Async Query API session to use (can be null)
   * @param backend sttp backend to use for submitting requests
   * @return sttp Response object of the submitted request
   */
  def executeAsyncQuery(language: String, query: String, sessionId: String, backend: SttpBackend[Identity, Any]) : Identity[Response[Either[ResponseException[String, JsError], JsValue]]] = {
    // Ensure the datasource exists before executing the query
    ensureDataSourceExists()
    var queryBody : String = null
    val escapedQuery = query.replaceAll("\n", "\\\\n")
    if (sessionId == null) {
      queryBody = "{\"datasource\": \"" + S3_DATASOURCE + "\", \"lang\": \"" + language + "\", \"query\": \"" + escapedQuery + "\"}"
    } else {
      queryBody = "{\"datasource\": \"" + S3_DATASOURCE + "\", \"lang\": \"" + language + "\", \"query\": \"" + escapedQuery + "\", \"sessionId\": \"" + sessionId + "\"}"
    }

    logInfo(s"Sending async query request to $OPENSEARCH_URL/_plugins/_async_query with body: $queryBody")
    // Check if OpenSearch is available
    try {
      val checkResponse = basicRequest.get(uri"$OPENSEARCH_URL")
        .auth.basic(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
        .send(backend)
      logInfo(s"OpenSearch availability check: ${checkResponse.code}")
      // Also check if the async query API is available
      val asyncQueryCheckResponse = basicRequest.get(uri"$OPENSEARCH_URL/_plugins/_async_query")
        .auth.basic(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
        .send(backend)
      logInfo(s"Async Query API availability check: ${asyncQueryCheckResponse.code}")
      // Check if the datasource exists
      val datasourceCheckResponse = basicRequest.get(uri"$OPENSEARCH_URL/_plugins/_query/datasource/$S3_DATASOURCE")
        .auth.basic(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
        .send(backend)
      logInfo(s"Datasource check: ${datasourceCheckResponse.code}")
    } catch {
      case e: Exception =>
        logError(s"Error checking OpenSearch availability: ${e.getMessage}", e)
    }

    basicRequest.post(uri"$OPENSEARCH_URL/_plugins/_async_query")
      .auth.basic(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
      .contentType("application/json")
      .body(queryBody, "UTF-8")
      .response(asJson[JsValue])
      .send(backend)
  }

  /**
   * Retrieves the results of an Async Query API query. Will wait up to 30 seconds for the query to finish and make
   * the results available.
   *
   * @param queryId ID of the previously submitted query
   * @param backend sttp backend to use
   * @return results of the previously submitted query in JSON
   */
  def getAsyncResults(queryId: String, backend: SttpBackend[Identity, Any]): JsValue = {
    val endTime = System.currentTimeMillis() + 30000
    var attemptCount = 0

    while (System.currentTimeMillis() < endTime) {
      attemptCount += 1
      logInfo(s"Fetching async query results for queryId [$queryId], attempt $attemptCount")
      val response = basicRequest.get(uri"$OPENSEARCH_URL/_plugins/_async_query/$queryId")
        .auth.basic(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
        .contentType("application/json")
        .response(asJson[JsValue])
        .send(backend)

      if (response.isSuccess) {
        val responseBody = response.body.right.get
        logInfo(s"Async query response for queryId [$queryId]: $responseBody")
        val status = (responseBody \ "status").asOpt[String]

        if (status.isDefined) {
          logInfo(s"Query status for queryId [$queryId]: ${status.get}")
          if (status.get == "SUCCESS") {
            return responseBody
          } else if (status.get == "FAILED") {
            val error = (responseBody \ "error").asOpt[String].getOrElse("Unknown error")
            logError(s"Async query failed for queryId [$queryId]: $error")
            throw new IllegalStateException(s"Unable to get async query results [$queryId]: $error")
          }
        } else {
          logInfo(s"No status found in response for queryId [$queryId]")
        }
      } else {
        logError(s"Failed to get async query results for queryId [$queryId]: ${response.body}")
      }

      Thread.sleep(500)
    }

    throw new IllegalStateException(s"Unable to get async query results (timeout) [$queryId] after $attemptCount attempts")
  }
}
