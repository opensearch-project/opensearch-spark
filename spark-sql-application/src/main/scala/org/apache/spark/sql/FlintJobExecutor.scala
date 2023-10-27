/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.util.Locale

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.opensearch.flint.core.FlintClient
import org.opensearch.flint.core.metadata.FlintMetadata
import play.api.libs.json.{JsArray, JsBoolean, JsObject, Json, JsString, JsValue}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.FlintJob.{getFormattedData, handleIndexNotFoundException, isSuperset, logError, logInfo}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.{RealTimeProvider, TimeProvider}

trait FlintJobExecutor {
  this: Logging =>

  var currentTimeProvider: TimeProvider = new RealTimeProvider()

  def createSparkConf(): SparkConf = {
    new SparkConf()
      .setAppName(getClass.getSimpleName)
      .set(
        "spark.sql.extensions",
        "org.opensearch.flint.spark.FlintPPLSparkExtensions,org.opensearch.flint.spark.FlintSparkExtensions")
  }

  def createSparkSession(conf: SparkConf): SparkSession = {
    SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
  }

  def writeData(resultData: DataFrame, resultIndex: String): Unit = {
    resultData.write
      .format("flint")
      .mode("append")
      .save(resultIndex)
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
      timeProvider: TimeProvider): DataFrame = {
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

    // Create the data rows
    val rows = Seq(
      (
        resultToSave,
        resultSchemaToSave,
        sys.env.getOrElse("SERVERLESS_EMR_JOB_ID", "unknown"),
        sys.env.getOrElse("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown"),
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
        sys.env.getOrElse("SERVERLESS_EMR_JOB_ID", "unknown"),
        sys.env.getOrElse("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown"),
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
    // The enabled setting, which can be applied only to the top-level mapping definition and to object fields,
    val mapping =
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

    try {
      val existingSchema = osClient.getIndexMetadata(resultIndex)
      if (!isSuperset(existingSchema, mapping)) {
        Left(s"The mapping of $resultIndex is incorrect.")
      } else {
        Right(())
      }
    } catch {
      case e: IllegalStateException
          if e.getCause().getMessage().contains("index_not_found_exception") =>
        handleIndexNotFoundException(osClient, resultIndex, mapping)
      case e: Exception =>
        val error = s"Failed to verify existing mapping: ${e.getMessage}"
        logError(error, e)
        Left(error)
    }
  }

  def handleIndexNotFoundException(
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

  def executeQuery(
      spark: SparkSession,
      query: String,
      dataSource: String,
      queryId: String,
      sessionId: String): DataFrame = {
    // Execute SQL query
    val startTime = System.currentTimeMillis()
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
      currentTimeProvider)
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
          "Fail to run query. Cause",
          spark,
          dataSource,
          query,
          queryId,
          sessionId)
      case r: Exception =>
        handleQueryException(
          r,
          "Fail to write result, cause",
          spark,
          dataSource,
          query,
          queryId,
          sessionId)
    }
  }
}
