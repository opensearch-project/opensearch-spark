/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.util.Locale

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.apache.commons.text.StringEscapeUtils.unescapeJava
import org.opensearch.flint.core.IRestHighLevelClient
import org.opensearch.flint.core.metrics.MetricConstants
import org.opensearch.flint.core.metrics.MetricsUtil.incrementCounter
import play.api.libs.json._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.types._
import org.apache.spark.sql.util._

trait FlintJobExecutor {
  this: Logging =>

  var currentTimeProvider: TimeProvider = new RealTimeProvider()
  var threadPoolFactory: ThreadPoolFactory = new DefaultThreadPoolFactory()
  var envinromentProvider: EnvironmentProvider = new RealEnvironment()
  var enableHiveSupport: Boolean = true
  // termiante JVM in the presence non-deamon thread before exiting
  var terminateJVM = true

  // The enabled setting, which can be applied only to the top-level mapping definition and to object fields,
  val resultIndexMapping =
    """{
      "dynamic": false,
      "properties": {
        "result": {
          "type": "object",
          "enabled": false
        },
        "schema": {
          "type": "object",
          "enabled": false
        },
        "jobRunId": {
          "type": "keyword"
        },
        "applicationId": {
          "type": "keyword"
        },
        "dataSourceName": {
          "type": "keyword"
        },
        "status": {
          "type": "keyword"
        },
        "queryId": {
           "type": "keyword"
        },
        "queryText": {
           "type": "text"
        },
        "sessionId": {
           "type": "keyword"
        },
        "updateTime": {
           "type": "date",
           "format": "strict_date_time||epoch_millis"
        },
        "error": {
          "type": "text"
        },
        "queryRunTime" : {
          "type" : "long"
        }
      }
    }""".stripMargin

  def createSparkConf(): SparkConf = {
    new SparkConf()
      .setAppName(getClass.getSimpleName)
      .set(
        "spark.sql.extensions",
        "org.opensearch.flint.spark.FlintPPLSparkExtensions,org.opensearch.flint.spark.FlintSparkExtensions")
  }

  /*
   * Override dynamicAllocation.maxExecutors with streaming maxExecutors. more detail at
   * https://github.com/opensearch-project/opensearch-spark/issues/324
   */
  def configDYNMaxExecutors(conf: SparkConf, jobType: String): Unit = {
    if (jobType.equalsIgnoreCase("streaming")) {
      conf.set(
        "spark.dynamicAllocation.maxExecutors",
        conf
          .get("spark.flint.streaming.dynamicAllocation.maxExecutors", "10"))
    }
  }

  def createSparkSession(conf: SparkConf): SparkSession = {
    val builder = SparkSession.builder().config(conf)
    if (enableHiveSupport) {
      builder.enableHiveSupport()
    }
    builder.getOrCreate()
  }

  private def writeData(resultData: DataFrame, resultIndex: String): Unit = {
    try {
      resultData.write
        .format("flint")
        .mode("append")
        .save(resultIndex)
      IRestHighLevelClient.recordOperationSuccess(
        MetricConstants.RESULT_METADATA_WRITE_METRIC_PREFIX)
    } catch {
      case e: Exception =>
        IRestHighLevelClient.recordOperationFailure(
          MetricConstants.RESULT_METADATA_WRITE_METRIC_PREFIX,
          e)
    }
  }

  /**
   * writes the DataFrame to the specified Elasticsearch index, and createIndex creates an index
   * with the given mapping if it does not exist.
   * @param resultData
   *   data to write
   * @param resultIndex
   *   result index
   * @param osClient
   *   OpenSearch client
   */
  def writeDataFrameToOpensearch(
      resultData: DataFrame,
      resultIndex: String,
      osClient: OSClient): Unit = {
    if (osClient.doesIndexExist(resultIndex)) {
      writeData(resultData, resultIndex)
    } else {
      createResultIndex(osClient, resultIndex, resultIndexMapping)
      writeData(resultData, resultIndex)
    }
  }

  /**
   * Create a new formatted dataframe with json result, json schema and EMR_STEP_ID.
   *
   * @param result
   *   sql query result dataframe
   * @param spark
   *   spark session
   * @return
   *   dataframe with result, schema and emr step id
   */
  def getFormattedData(
      result: DataFrame,
      spark: SparkSession,
      dataSource: String,
      queryId: String,
      query: String,
      sessionId: String,
      startTime: Long,
      timeProvider: TimeProvider,
      cleaner: Cleaner): DataFrame = {
    // Create the schema dataframe
    val schemaRows = result.schema.fields.map { field =>
      Row(field.name, field.dataType.typeName)
    }
    val resultSchema = spark.createDataFrame(
      spark.sparkContext.parallelize(schemaRows),
      StructType(
        Seq(
          StructField("column_name", StringType, nullable = false),
          StructField("data_type", StringType, nullable = false))))

    // Define the data schema
    val schema = StructType(
      Seq(
        StructField("result", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("schema", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("jobRunId", StringType, nullable = true),
        StructField("applicationId", StringType, nullable = true),
        StructField("dataSourceName", StringType, nullable = true),
        StructField("status", StringType, nullable = true),
        StructField("error", StringType, nullable = true),
        StructField("queryId", StringType, nullable = true),
        StructField("queryText", StringType, nullable = true),
        StructField("sessionId", StringType, nullable = true),
        // number is not nullable
        StructField("updateTime", LongType, nullable = false),
        StructField("queryRunTime", LongType, nullable = true)))

    val resultToSave = result.toJSON.collect.toList
      .map(_.replaceAll("'", "\\\\'").replaceAll("\"", "'"))

    val resultSchemaToSave = resultSchema.toJSON.collect.toList.map(_.replaceAll("\"", "'"))
    val endTime = timeProvider.currentEpochMillis()

    // https://github.com/opensearch-project/opensearch-spark/issues/302. Clean shuffle data
    // after consumed the query result. Streaming query shuffle data is cleaned after each
    // microBatch execution.
    cleaner.cleanUp(spark)

    // Create the data rows
    val rows = Seq(
      (
        resultToSave,
        resultSchemaToSave,
        envinromentProvider.getEnvVar("SERVERLESS_EMR_JOB_ID", "unknown"),
        envinromentProvider.getEnvVar("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown"),
        dataSource,
        "SUCCESS",
        "",
        queryId,
        query,
        sessionId,
        endTime,
        endTime - startTime))

    // Create the DataFrame for data
    spark.createDataFrame(rows).toDF(schema.fields.map(_.name): _*)
  }

  def getFailedData(
      spark: SparkSession,
      dataSource: String,
      error: String,
      queryId: String,
      query: String,
      sessionId: String,
      startTime: Long,
      timeProvider: TimeProvider): DataFrame = {

    // Define the data schema
    val schema = StructType(
      Seq(
        StructField("result", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("schema", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("jobRunId", StringType, nullable = true),
        StructField("applicationId", StringType, nullable = true),
        StructField("dataSourceName", StringType, nullable = true),
        StructField("status", StringType, nullable = true),
        StructField("error", StringType, nullable = true),
        StructField("queryId", StringType, nullable = true),
        StructField("queryText", StringType, nullable = true),
        StructField("sessionId", StringType, nullable = true),
        // number is not nullable
        StructField("updateTime", LongType, nullable = false),
        StructField("queryRunTime", LongType, nullable = true)))

    val endTime = timeProvider.currentEpochMillis()

    // Create the data rows
    val rows = Seq(
      (
        null,
        null,
        envinromentProvider.getEnvVar("SERVERLESS_EMR_JOB_ID", "unknown"),
        envinromentProvider.getEnvVar("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown"),
        dataSource,
        "FAILED",
        error,
        queryId,
        query,
        sessionId,
        endTime,
        endTime - startTime))

    // Create the DataFrame for data
    spark.createDataFrame(rows).toDF(schema.fields.map(_.name): _*)
  }

  def isSuperset(input: String, mapping: String): Boolean = {

    /**
     * Determines whether one JSON structure is a superset of another.
     *
     * This method checks if the `input` JSON structure contains all the fields and values present
     * in the `mapping` JSON structure. The comparison is recursive and structure-sensitive,
     * ensuring that nested objects and arrays are also compared accurately.
     *
     * Additionally, this method accommodates the edge case where boolean values in the JSON are
     * represented as strings (e.g., "true" or "false" instead of true or false). This is handled
     * by performing a case-insensitive comparison of string representations of boolean values.
     *
     * @param input
     *   The input JSON structure as a String.
     * @param mapping
     *   The mapping JSON structure as a String.
     * @return
     *   A Boolean value indicating whether the `input` JSON structure is a superset of the
     *   `mapping` JSON structure.
     */
    def compareJson(inputJson: JsValue, mappingJson: JsValue): Boolean = {
      (inputJson, mappingJson) match {
        case (JsObject(inputFields), JsObject(mappingFields)) =>
          mappingFields.forall { case (key, value) =>
            inputFields
              .get(key)
              .exists(inputValue => compareJson(inputValue, value))
          }
        case (JsArray(inputValues), JsArray(mappingValues)) =>
          mappingValues.forall(mappingValue =>
            inputValues.exists(inputValue => compareJson(inputValue, mappingValue)))
        case (JsString(inputValue), JsString(mappingValue))
            if (inputValue.toLowerCase(Locale.ROOT) == "true" ||
              inputValue.toLowerCase(Locale.ROOT) == "false") &&
              (mappingValue.toLowerCase(Locale.ROOT) == "true" ||
                mappingValue.toLowerCase(Locale.ROOT) == "false") =>
          inputValue.toLowerCase(Locale.ROOT) == mappingValue.toLowerCase(Locale.ROOT)
        case (JsBoolean(inputValue), JsString(mappingValue))
            if mappingValue.toLowerCase(Locale.ROOT) == "true" ||
              mappingValue.toLowerCase(Locale.ROOT) == "false" =>
          inputValue.toString.toLowerCase(Locale.ROOT) == mappingValue
            .toLowerCase(Locale.ROOT)
        case (JsString(inputValue), JsBoolean(mappingValue))
            if inputValue.toLowerCase(Locale.ROOT) == "true" ||
              inputValue.toLowerCase(Locale.ROOT) == "false" =>
          inputValue.toLowerCase(Locale.ROOT) == mappingValue.toString
            .toLowerCase(Locale.ROOT)
        case (inputValue, mappingValue) =>
          inputValue == mappingValue
      }
    }

    val inputJson = Json.parse(input)
    val mappingJson = Json.parse(mapping)

    compareJson(inputJson, mappingJson)
  }

  def checkAndCreateIndex(osClient: OSClient, resultIndex: String): Either[String, Unit] = {
    try {
      val existingSchema = osClient.getIndexMetadata(resultIndex)
      if (!isSuperset(existingSchema, resultIndexMapping)) {
        Left(s"The mapping of $resultIndex is incorrect.")
      } else {
        Right(())
      }
    } catch {
      case e: IllegalStateException
          if e.getCause != null &&
            e.getCause.getMessage.contains("index_not_found_exception") =>
        createResultIndex(osClient, resultIndex, resultIndexMapping)
      case e: InterruptedException =>
        val error = s"Interrupted by the main thread: ${e.getMessage}"
        Thread.currentThread().interrupt() // Preserve the interrupt status
        logError(error, e)
        Left(error)
      case e: Exception =>
        val error = s"Failed to verify existing mapping: ${e.getMessage}"
        logError(error, e)
        Left(error)
    }
  }

  def createResultIndex(
      osClient: OSClient,
      resultIndex: String,
      mapping: String): Either[String, Unit] = {
    try {
      logInfo(s"create $resultIndex")
      osClient.createIndex(resultIndex, mapping)
      logInfo(s"create $resultIndex successfully")
      Right(())
    } catch {
      case e: Exception =>
        val error = s"Failed to create result index $resultIndex"
        logError(error, e)
        Left(error)
    }
  }

  /**
   * Unescape the query string which is escaped for EMR spark submit parameter parsing. Ref:
   * https://github.com/opensearch-project/sql/pull/2587
   */
  def unescapeQuery(query: String): String = {
    unescapeJava(query)
  }

  def executeQuery(
      spark: SparkSession,
      query: String,
      dataSource: String,
      queryId: String,
      sessionId: String,
      streaming: Boolean): DataFrame = {
    // Execute SQL query
    val startTime = System.currentTimeMillis()
    // we have to set job group in the same thread that started the query according to spark doc
    spark.sparkContext.setJobGroup(queryId, "Job group for " + queryId, interruptOnCancel = true)
    val result: DataFrame = spark.sql(query)
    // Get Data
    getFormattedData(
      result,
      spark,
      dataSource,
      queryId,
      query,
      sessionId,
      startTime,
      currentTimeProvider,
      CleanerFactory.cleaner(streaming))
  }

  private def handleQueryException(
      e: Exception,
      message: String,
      spark: SparkSession,
      dataSource: String,
      query: String,
      queryId: String,
      sessionId: String): String = {
    val error = s"$message: ${e.getMessage}"
    logError(error, e)
    error
  }

  def getRootCause(e: Throwable): Throwable = {
    if (e.getCause == null) e
    else getRootCause(e.getCause)
  }

  def processQueryException(
      ex: Exception,
      spark: SparkSession,
      dataSource: String,
      query: String,
      queryId: String,
      sessionId: String): String = {
    getRootCause(ex) match {
      case r: ParseException =>
        handleQueryException(r, "Syntax error", spark, dataSource, query, queryId, sessionId)
      case r: AmazonS3Exception =>
        incrementCounter(MetricConstants.S3_ERR_CNT_METRIC)
        handleQueryException(
          r,
          "Fail to read data from S3. Cause",
          spark,
          dataSource,
          query,
          queryId,
          sessionId)
      case r: AnalysisException =>
        handleQueryException(
          r,
          "Fail to analyze query. Cause",
          spark,
          dataSource,
          query,
          queryId,
          sessionId)
      case r: SparkException =>
        handleQueryException(
          r,
          "Spark exception. Cause",
          spark,
          dataSource,
          query,
          queryId,
          sessionId)
      case r: Exception =>
        handleQueryException(
          r,
          "Fail to run query, cause",
          spark,
          dataSource,
          query,
          queryId,
          sessionId)
    }
  }
}
