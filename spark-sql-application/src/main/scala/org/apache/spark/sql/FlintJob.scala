/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

// defined in spark package so that I can use ThreadUtils
package org.apache.spark.sql

import java.util.Locale

import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration.{Duration, MINUTES}

import org.opensearch.ExceptionsHelper
import org.opensearch.client.{RequestOptions, RestHighLevelClient}
import org.opensearch.cluster.metadata.MappingMetadata
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder, FlintOptions}
import org.opensearch.flint.core.metadata.FlintMetadata
import play.api.libs.json._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.util.ThreadUtils

/**
 * Spark SQL Application entrypoint
 *
 * @param args
 *   (0) sql query
 * @param args
 *   (1) opensearch index name
 * @return
 *   write sql query result to given opensearch index
 */
object FlintJob extends Logging {
  def main(args: Array[String]): Unit = {
    // Validate command line arguments
    if (args.length != 2) {
      throw new IllegalArgumentException("Usage: FlintJob <query> <resultIndex>")
    }

    val Array(query, resultIndex) = args

    val conf = createSparkConf()
    val wait = conf.get("spark.flint.job.type", "continue")
    val dataSource = conf.get("spark.flint.datasource.name", "")
    val spark = createSparkSession(conf)

    val threadPool = ThreadUtils.newDaemonFixedThreadPool(1, "check-create-index")
    implicit val executionContext = ExecutionContext.fromExecutor(threadPool)

    var dataToWrite: Option[DataFrame] = None
    try {
      // osClient needs spark session to be created first. Otherwise, we will have connection
      // exception from EMR-S to OS.
      val osClient = new OSClient(FlintSparkConf().flintOptions())
      val futureMappingCheck = Future {
        checkAndCreateIndex(osClient, resultIndex)
      }
      val data = executeQuery(spark, query, dataSource)

      val mappingCheckResult = ThreadUtils.awaitResult(futureMappingCheck, Duration(1, MINUTES))
      dataToWrite = Some(mappingCheckResult match {
        case Right(_) => data
        case Left(error) => getFailedData(spark, dataSource, error)
      })
    } catch {
      case e: TimeoutException =>
        val error = "Future operations timed out"
        logError(error, e)
        dataToWrite = Some(getFailedData(spark, dataSource, error))
      case e: Exception =>
        val error = "Fail to verify existing mapping or write result"
        logError(error, e)
        dataToWrite = Some(getFailedData(spark, dataSource, error))
    } finally {
      dataToWrite.foreach(df => writeData(df, resultIndex))
      // Stop SparkSession if it is not streaming job
      if (wait.equalsIgnoreCase("streaming")) {
        spark.streams.awaitAnyTermination()
      } else {
        spark.stop()
      }

      threadPool.shutdown()
    }
  }

  def createSparkConf(): SparkConf = {
    new SparkConf()
      .setAppName("FlintJob")
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
  def getFormattedData(result: DataFrame, spark: SparkSession, dataSource: String): DataFrame = {
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
        StructField("error", StringType, nullable = true)))

    // Create the data rows
    val rows = Seq(
      (
        result.toJSON.collect.toList
          .map(_.replaceAll("'", "\\\\'").replaceAll("\"", "'")),
        resultSchema.toJSON.collect.toList.map(_.replaceAll("\"", "'")),
        sys.env.getOrElse("SERVERLESS_EMR_JOB_ID", "unknown"),
        sys.env.getOrElse("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown"),
        dataSource,
        "SUCCESS",
        ""))

    // Create the DataFrame for data
    spark.createDataFrame(rows).toDF(schema.fields.map(_.name): _*)
  }

  def getFailedData(spark: SparkSession, dataSource: String, error: String): DataFrame = {

    // Define the data schema
    val schema = StructType(
      Seq(
        StructField("result", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("schema", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("jobRunId", StringType, nullable = true),
        StructField("applicationId", StringType, nullable = true),
        StructField("dataSourceName", StringType, nullable = true),
        StructField("status", StringType, nullable = true),
        StructField("error", StringType, nullable = true)))

    // Create the data rows
    val rows = Seq(
      (
        null,
        null,
        sys.env.getOrElse("SERVERLESS_EMR_JOB_ID", "unknown"),
        sys.env.getOrElse("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown"),
        dataSource,
        "FAILED",
        error))

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
          logInfo(s"Comparing objects: $inputFields vs $mappingFields")
          mappingFields.forall { case (key, value) =>
            inputFields
              .get(key)
              .exists(inputValue => compareJson(inputValue, value))
          }
        case (JsArray(inputValues), JsArray(mappingValues)) =>
          logInfo(s"Comparing arrays: $inputValues vs $mappingValues")
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
          "error": {
            "type": "text"
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
        try {
          osClient.createIndex(resultIndex, mapping)
          Right(())
        } catch {
          case e: Exception =>
            val error = s"Failed to create result index $resultIndex"
            logError(error, e)
            Left(error)
        }
      case e: Exception =>
        val error = "Failed to verify existing mapping"
        logError(error, e)
        Left(error)
    }
  }

  def executeQuery(spark: SparkSession, query: String, dataSource: String): DataFrame = {
    // Execute SQL query
    val result: DataFrame = spark.sql(query)
    // Get Data
    getFormattedData(result, spark, dataSource)
  }
}
