/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.mockito.ArgumentMatchersSugar
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkConfConstants.{DEFAULT_SQL_EXTENSIONS, SQL_EXTENSIONS_KEY}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{CleanerFactory, MockTimeProvider}

class FlintJobTest
    extends SparkFunSuite
    with MockitoSugar
    with ArgumentMatchersSugar
    with JobMatchers {
  private val jobId = "testJobId"
  private val applicationId = "testApplicationId"

  val spark =
    SparkSession.builder().appName("Test").master("local").getOrCreate()
  spark.conf.set(FlintSparkConf.JOB_TYPE.key, "streaming")
  // Define input dataframe
  val inputSchema = StructType(
    Seq(
      StructField("Letter", StringType, nullable = false),
      StructField("Number", IntegerType, nullable = false)))
  val inputRows = Seq(Row("A", 1), Row("B", 2), Row("C", 3))
  val input: DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(inputRows), inputSchema)

  test("Test getFormattedData method") {
    // Define expected dataframe
    val dataSourceName = "myGlueS3"
    val expectedSchema = StructType(
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
        StructField("jobType", StringType, nullable = true),
        StructField("updateTime", LongType, nullable = false),
        StructField("queryRunTime", LongType, nullable = false)))

    val currentTime: Long = System.currentTimeMillis()
    val queryRunTime: Long = 3000L

    val expectedRows = Seq(
      Row(
        Array(
          "{'Letter':'A','Number':1}",
          "{'Letter':'B','Number':2}",
          "{'Letter':'C','Number':3}"),
        Array(
          "{'column_name':'Letter','data_type':'string'}",
          "{'column_name':'Number','data_type':'integer'}"),
        jobId,
        applicationId,
        dataSourceName,
        "SUCCESS",
        "",
        "10",
        "select 1",
        "20",
        "streaming",
        currentTime,
        queryRunTime))
    val expected: DataFrame =
      spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), expectedSchema)

    // Compare the result
    val result =
      FlintJob.getFormattedData(
        applicationId,
        jobId,
        input,
        spark,
        dataSourceName,
        "10",
        "select 1",
        "20",
        currentTime - queryRunTime,
        new MockTimeProvider(currentTime),
        CleanerFactory.cleaner(false))
    assertEqualDataframe(expected, result)
  }

  test("test isSuperset") {
    // Note in input false has enclosed double quotes, while mapping just has false
    val input =
      """{"dynamic":"false","properties":{"result":{"type":"object"},"schema":{"type":"object"},
        |"applicationId":{"type":"keyword"},"jobRunId":{
        |"type":"keyword"},"dataSourceName":{"type":"keyword"},"status":{"type":"keyword"},
        |"error":{"type":"text"}}}
        |""".stripMargin
    val mapping =
      """{"dynamic":"false","properties":{"result":{"type":"object"},"schema":{"type":"object"}, "jobType":{"type": "keyword"},
        |"applicationId":{"type":"keyword"},"jobRunId":{
        |"type":"keyword"},"dataSourceName":{"type":"keyword"},"status":{"type":"keyword"},
        |"error":{"type":"text"}}}
        |""".stripMargin

    // Assert that input is a superset of mapping
    assert(FlintJob.isSuperset(input, mapping))

    // Assert that mapping is a superset of input
    assert(FlintJob.isSuperset(mapping, input))
  }

  test("default streaming query maxExecutors is 10") {
    val conf = spark.sparkContext.conf
    FlintJob.configDYNMaxExecutors(conf, "streaming")
    conf.get("spark.dynamicAllocation.maxExecutors") shouldBe "10"
  }

  test("override streaming query maxExecutors") {
    spark.sparkContext.conf.set("spark.flint.streaming.dynamicAllocation.maxExecutors", "30")
    FlintJob.configDYNMaxExecutors(spark.sparkContext.conf, "streaming")
    spark.sparkContext.conf.get("spark.dynamicAllocation.maxExecutors") shouldBe "30"
  }

  test("createSparkConf should set the app name and default SQL extensions") {
    val conf = FlintJob.createSparkConf()

    // Assert that the app name is set correctly
    assert(conf.get("spark.app.name") === "FlintJob$")

    // Assert that the default SQL extensions are set correctly
    assert(conf.get(SQL_EXTENSIONS_KEY) === DEFAULT_SQL_EXTENSIONS)
  }

  test(
    "createSparkConf should not use defaultExtensions if spark.sql.extensions is already set") {
    val customExtension = "my.custom.extension"
    // Set the spark.sql.extensions property before calling createSparkConf
    System.setProperty(SQL_EXTENSIONS_KEY, customExtension)

    try {
      val conf = FlintJob.createSparkConf()
      assert(conf.get(SQL_EXTENSIONS_KEY) === customExtension)
    } finally {
      // Clean up the system property after the test
      System.clearProperty(SQL_EXTENSIONS_KEY)
    }
  }
}
