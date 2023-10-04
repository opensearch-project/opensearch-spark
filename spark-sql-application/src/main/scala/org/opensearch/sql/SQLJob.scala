/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

/**
 * Spark SQL Application entrypoint
 *
 * @param args (0)
 *             sql query
 * @param args (1)
 *             opensearch index name
 * @param args (2-6)
 *             opensearch connection values required for flint-integration jar.
 *             host, port, scheme, auth, region respectively.
 * @return
 * write sql query result to given opensearch index
 */
case class JobConfig(
    extensions: String,
    query: String,
    index: String,
    host: String,
    port: String,
    scheme: String,
    auth: String,
    region: String
  )

object SQLJob {
  private def parseArgs(args: Array[String]): JobConfig = {
    if (args.length < 8) {
      throw new IllegalArgumentException("Insufficient arguments provided! - args: [extensions, query, index, host, port, scheme, auth, region]")
    }

    JobConfig(
      extensions = args(0),
      query = args(1),
      index = args(2),
      host = args(3),
      port = args(4),
      scheme = args(5),
      auth = args(6),
      region = args(7)
    )
  }

  def createSparkConf(config: JobConfig): SparkConf = {
    new SparkConf()
      .setAppName("SQLJob")
      .set("spark.sql.extensions", config.extensions)
      .set("spark.datasource.flint.host", config.host)
      .set("spark.datasource.flint.port", config.port)
      .set("spark.datasource.flint.scheme", config.scheme)
      .set("spark.datasource.flint.auth", config.auth)
      .set("spark.datasource.flint.region", config.region)
  }
  def main(args: Array[String]) {
    val config = parseArgs(args)

    val sparkConf = createSparkConf(config)
 

    // Create a SparkSession
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    try {
      // Execute SQL query
      val result: DataFrame = spark.sql(config.query)

      // Get Data
      val data = getFormattedData(result, spark)

      // Write data to OpenSearch index
      val aos = Map(
        "host" -> config.host,
        "port" -> config.port,
        "scheme" -> config.scheme,
        "auth" -> config.auth,
        "region" -> config.region)

      data.write
        .format("flint")
        .options(aos)
        .mode("append")
        .save(config.index)

    } finally {
      // Stop SparkSession
      spark.stop()
    }
  }

  /**
   * Create a new formatted dataframe with json result, json schema and EMR_STEP_ID.
   *
   * @param result
   * sql query result dataframe
   * @param spark
   * spark session
   * @return
   * dataframe with result, schema and emr step id
   */
  def getFormattedData(result: DataFrame, spark: SparkSession): DataFrame = {
    // Create the schema dataframe
    val schemaRows = result.schema.fields.map { field =>
      Row(field.name, field.dataType.typeName)
    }
    val resultSchema = spark.createDataFrame(spark.sparkContext.parallelize(schemaRows),
      StructType(Seq(
        StructField("column_name", StringType, nullable = false),
        StructField("data_type", StringType, nullable = false))))

    // Define the data schema
    val schema = StructType(Seq(
      StructField("result", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("schema", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("stepId", StringType, nullable = true),
      StructField("applicationId", StringType, nullable = true)))

    // Create the data rows
    val rows = Seq((
      result.toJSON.collect.toList.map(_.replaceAll("'", "\\\\'").replaceAll("\"", "'")),
      resultSchema.toJSON.collect.toList.map(_.replaceAll("\"", "'")),
      sys.env.getOrElse("EMR_STEP_ID", "unknown"),
      spark.sparkContext.applicationId))

    // Create the DataFrame for data
    spark.createDataFrame(rows).toDF(schema.fields.map(_.name): _*)
  }
}
