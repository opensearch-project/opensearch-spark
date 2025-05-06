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
    try {
      logInfo("Starting docker cluster setup")

      // Step 1: Load environment properties
      logInfo("Loading docker environment properties")
      val dockerEnv = new Properties()
      val envFile = new File(DOCKER_INTEG_DIR, ".env")
      logInfo(s"Reading environment from ${envFile.getAbsolutePath()}")
      if (!envFile.exists()) {
        throw new IllegalStateException(s"Environment file not found: ${envFile.getAbsolutePath()}")
      }
      dockerEnv.load(new FileInputStream(envFile))
      OPENSEARCH_URL = "http://localhost:" + dockerEnv.getProperty("OPENSEARCH_PORT")
      OPENSEARCH_PASSWORD = dockerEnv.getProperty("OPENSEARCH_ADMIN_PASSWORD")
      SPARK_CONNECT_PORT = Integer.parseInt(dockerEnv.getProperty("SPARK_CONNECT_PORT"))
      S3_ACCESS_KEY = dockerEnv.getProperty("S3_ACCESS_KEY")
      S3_SECRET_KEY = dockerEnv.getProperty("S3_SECRET_KEY")
      logInfo(s"Environment loaded: OPENSEARCH_URL=$OPENSEARCH_URL, SPARK_CONNECT_PORT=$SPARK_CONNECT_PORT")

      // Step 2: Clean up existing docker volumes
      logInfo("Cleaning up existing docker volumes")
      val cmdWithArgs = List("docker", "volume", "rm") ++ DOCKER_VOLUMES
      logInfo(s"Executing command: ${cmdWithArgs.mkString(" ")}")
      val deleteDockerVolumesProcess = new ProcessBuilder(cmdWithArgs.toArray: _*).start()
      val volumeDeleteCompleted = deleteDockerVolumesProcess.waitFor(10, TimeUnit.SECONDS)
      if (!volumeDeleteCompleted) {
        logWarning("Docker volume deletion timed out, proceeding anyway")
      } else {
        val exitCode = deleteDockerVolumesProcess.exitValue()
        logInfo(s"Docker volume deletion completed with exit code: $exitCode")
      }

      // Step 3: Start docker containers
      logInfo("Starting docker containers")
      val dockerComposeDir = new File(DOCKER_INTEG_DIR)
      logInfo(s"Docker compose directory: ${dockerComposeDir.getAbsolutePath()}")
      if (!dockerComposeDir.exists()) {
        throw new IllegalStateException(s"Docker compose directory not found: ${dockerComposeDir.getAbsolutePath()}")
      }
      // Check if docker-compose.yml exists
      val dockerComposeFile = new File(dockerComposeDir, "docker-compose.yml")
      if (!dockerComposeFile.exists()) {
        throw new IllegalStateException(s"docker-compose.yml not found at ${dockerComposeFile.getAbsolutePath()}")
      }
      // First check if all required JAR files exist
      val pplJarPath = dockerEnv.getProperty("PPL_JAR")
      val flintJarPath = dockerEnv.getProperty("FLINT_JAR")
      val sqlAppJarPath = dockerEnv.getProperty("SQL_APP_JAR")

      val pplJarFile = new File(pplJarPath)
      val flintJarFile = new File(flintJarPath)
      val sqlAppJarFile = new File(sqlAppJarPath)

      if (!pplJarFile.exists()) {
        logError(s"PPL JAR file not found at ${pplJarFile.getAbsolutePath}")
        throw new IllegalStateException(s"PPL JAR file not found at ${pplJarFile.getAbsolutePath}")
      }
      if (!flintJarFile.exists()) {
        logError(s"Flint JAR file not found at ${flintJarFile.getAbsolutePath}")
        throw new IllegalStateException(s"Flint JAR file not found at ${flintJarFile.getAbsolutePath}")
      }
      if (!sqlAppJarFile.exists()) {
        logError(s"SQL App JAR file not found at ${sqlAppJarFile.getAbsolutePath}")
        throw new IllegalStateException(s"SQL App JAR file not found at ${sqlAppJarFile.getAbsolutePath}")
      }

      logInfo(s"All required JAR files found: PPL=${pplJarFile.getAbsolutePath}, Flint=${flintJarFile.getAbsolutePath}, SQL=${sqlAppJarFile.getAbsolutePath}")

      // The script is already executable, so we don't need to chmod it

      // Try running docker-compose with a clean environment
      logInfo("Running docker-compose down first to clean up any existing containers")
      val cleanupProcess = new ProcessBuilder("docker", "compose", "down", "--volumes", "--remove-orphans")
        .directory(dockerComposeDir)
        .start()

      val cleanupCompleted = cleanupProcess.waitFor(5, TimeUnit.MINUTES)
      if (!cleanupCompleted) {
        logWarning("Docker compose cleanup timed out, proceeding anyway")
      } else {
        val exitCode = cleanupProcess.exitValue()
        logInfo(s"Docker compose cleanup completed with exit code: $exitCode")
      }

      // Use docker-compose directly with verbose output
      logInfo("Using docker-compose command")
      val dockerComposeCommand = Array("docker", "compose", "up", "-d")

      // Add environment variables to the process
      val pb = new ProcessBuilder(dockerComposeCommand: _*)
        .directory(dockerComposeDir)

      // Debug: Print the command
      logInfo(s"Executing command: ${dockerComposeCommand.mkString(" ")} in directory ${dockerComposeDir.getAbsolutePath()}")

      val dockerProcess = pb.start()
      // Capture and log docker compose output
      var stopReading = false
      val logThread = new Thread() {
        override def run(): Unit = {
          val reader = new BufferedReader(new InputStreamReader(dockerProcess.getInputStream))
          val errorReader = new BufferedReader(new InputStreamReader(dockerProcess.getErrorStream))
          try {
            var line = reader.readLine()
            while (!stopReading && line != null) {
              logInfo("Docker output: " + line)
              line = reader.readLine()
            }
            // Also read error stream
            line = errorReader.readLine()
            while (!stopReading && line != null) {
              logError("Docker error: " + line)
              line = errorReader.readLine()
            }
          } catch {
            case e: Exception =>
              logError("Error reading docker process output", e)
          }
        }
      }
      logThread.start()
      logInfo("Waiting for docker containers to start (timeout: 20 minutes)")
      val completed = dockerProcess.waitFor(20, TimeUnit.MINUTES)
      stopReading = true
      if (!completed) {
        throw new IllegalStateException("Docker container startup timed out after 20 minutes")
      }

      val exitCode = dockerProcess.exitValue()
      if (exitCode != 0) {
        logError(s"Docker compose failed with exit code: $exitCode")
        throw new IllegalStateException(s"Docker compose failed with exit code: $exitCode")
      }

      logInfo("Docker containers started successfully")

      // Step 4: Wait for services to be ready
      logInfo("Waiting for services to be ready")
      try {
        logInfo("Waiting for MinIO service (localhost:9000)")
        waitForService("localhost", 9000, 120000)  // MinIO - increased timeout to 2 minutes
        logInfo("MinIO service is ready")
        logInfo(s"Waiting for Spark service (localhost:$SPARK_CONNECT_PORT)")
        waitForService("localhost", SPARK_CONNECT_PORT, 120000)  // Spark - increased timeout to 2 minutes
        logInfo("Spark service is ready")
        logInfo(s"Waiting for OpenSearch service (localhost:${dockerEnv.getProperty("OPENSEARCH_PORT").toInt})")
        waitForService("localhost", dockerEnv.getProperty("OPENSEARCH_PORT").toInt, 120000)  // OpenSearch - increased timeout to 2 minutes
        logInfo("OpenSearch service is ready")
      } catch {
        case e: Exception =>
          logError("Failed waiting for services to be ready", e)
          throw e
      }

      // Step 5: Wait for S3 service to be available
      logInfo("Waiting for S3 service to be available")
      if (!waitForS3Available()) {
        throw new IllegalStateException("S3 service did not become available after multiple attempts")
      }
      logInfo("S3 service is available")

      // Step 6: Create required buckets
      logInfo("Creating required S3 buckets")
      try {
        logInfo(s"Ensuring bucket '$INTEG_TEST_BUCKET' exists")
        ensureBucketExists(INTEG_TEST_BUCKET)
        logInfo(s"Successfully verified/created bucket '$INTEG_TEST_BUCKET'")
        logInfo(s"Ensuring bucket '$TEST_RESOURCES_BUCKET' exists")
        ensureBucketExists(TEST_RESOURCES_BUCKET)
        logInfo(s"Successfully verified/created bucket '$TEST_RESOURCES_BUCKET'")
        // Step 7: Ensure the datasource exists for Async Query API
        logInfo("Ensuring datasource exists for Async Query API")
        if (!ensureDataSourceExists()) {
          logError("Failed to create or verify datasource for Async Query API")
        } else {
          logInfo("Datasource for Async Query API is ready")
        }
      } catch {
        case e: Exception =>
          logError(s"Failed to ensure buckets exist: ${e.getMessage}", e)
          throw e
      }
      // Step 8: Create tables and indices
      logInfo("Creating Spark tables")
      createTables()
      logInfo("Creating OpenSearch indices")
      createIndices()
      logInfo("Docker cluster setup completed successfully")
    } catch {
      case e: Exception =>
        logError("Exception during beforeAll setup", e)
        throw e
    }
  }

  /**
   * Shuts down the integration test docker cluster.
   */
  override def afterAll(): Unit = {
    logInfo("Stopping docker cluster")
    waitForSparkSubmitCompletion()

    try {
      // First attempt to stop containers gracefully
      // Check if we're using Docker Compose V2 or V1
      val checkDockerComposeVersion = new ProcessBuilder("docker", "compose", "version")
        .start()
      val isDockerComposeV2 = checkDockerComposeVersion.waitFor(5, TimeUnit.SECONDS) &&
                              checkDockerComposeVersion.exitValue() == 0
      // Use the appropriate Docker Compose command based on version
      val dockerComposeCommand = if (isDockerComposeV2) {
        Array("docker", "compose", "down")
      } else {
        Array("docker", "compose", "down")
      }
      val dockerProcess = new ProcessBuilder(dockerComposeCommand: _*)
        .directory(new File(DOCKER_INTEG_DIR))
        .start()
      // Capture output for debugging
      val errorReader = new BufferedReader(new InputStreamReader(dockerProcess.getErrorStream))
      val outputReader = new BufferedReader(new InputStreamReader(dockerProcess.getInputStream))
      val completed = dockerProcess.waitFor(10, TimeUnit.MINUTES)
      if (!completed) {
        logWarning("Docker compose down command timed out, proceeding anyway")
      } else if (dockerProcess.exitValue() != 0) {
        // Read and log error output
        var line = errorReader.readLine()
        while (line != null) {
          logError("Docker error: " + line)
          line = errorReader.readLine()
        }
        // Try a more forceful approach if the first one failed
        logWarning("First docker compose down attempt failed, trying with force option")
        val forceCommand = if (isDockerComposeV2) {
          Array("docker", "compose", "down", "--remove-orphans", "-v")
        } else {
          Array("docker", "compose", "down", "--remove-orphans", "-v")
        }
        val forceProcess = new ProcessBuilder(forceCommand: _*)
          .directory(new File(DOCKER_INTEG_DIR))
          .start()
        forceProcess.waitFor(5, TimeUnit.MINUTES)
      }
      // Close readers
      errorReader.close()
      outputReader.close()
      logInfo("Stopped docker cluster")
    } catch {
      case e: Exception =>
        logError("Exception while stopping docker cluster", e)
        // Don't fail the test suite just because cleanup had issues
    }
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
    var attemptCount = 0
    var lastError: Throwable = null
    while (!connected && System.currentTimeMillis() < endTime) {
      attemptCount += 1
      try {
        val socket = new java.net.Socket()
        // Increase connection timeout from 1 second to 5 seconds
        socket.connect(new java.net.InetSocketAddress(host, port), 5000)
        socket.close()
        connected = true
        logInfo(s"Successfully connected to $host:$port on attempt $attemptCount")
      } catch {
        case e: Exception =>
          lastError = e
          val timeLeft = endTime - System.currentTimeMillis()
          if (timeLeft > 0) {
            logInfo(s"Failed to connect to $host:$port on attempt $attemptCount. ${timeLeft}ms remaining. Error: ${e.getMessage}")
            // Increase sleep time between attempts
            Thread.sleep(3000)
          }
      }
    }
    if (!connected) {
      val errorMsg = s"Service $host:$port not available after ${timeoutMs}ms and $attemptCount attempts"
      logError(errorMsg, lastError)
      throw new IllegalStateException(errorMsg, lastError)
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
