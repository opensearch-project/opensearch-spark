/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class FlintJobTest extends SparkFunSuite with Matchers {

  val spark =
    SparkSession.builder().appName("Test").master("local").getOrCreate()

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
        StructField("error", StringType, nullable = true)
      ))
    val expectedRows = Seq(
      Row(
        Array(
          "{'Letter':'A','Number':1}",
          "{'Letter':'B','Number':2}",
          "{'Letter':'C','Number':3}"),
        Array(
          "{'column_name':'Letter','data_type':'string'}",
          "{'column_name':'Number','data_type':'integer'}"),
        "unknown",
        "unknown",
        dataSourceName,
        "SUCCESS",
        ""
      ))
    val expected: DataFrame =
      spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), expectedSchema)

    // Compare the result
    val result = FlintJob.getFormattedData(input, spark, dataSourceName)
    assertEqualDataframe(expected, result)
  }

  def assertEqualDataframe(expected: DataFrame, result: DataFrame): Unit = {
    assert(expected.schema === result.schema)
    assert(expected.collect() === result.collect())
  }

  test("test isSuperset") {
    // note in input false has enclosed double quotes, while mapping just has false
    val input =
      """{"dynamic":"false","properties":{"result":{"type":"object"},"schema":{"type":"object"},
        |"applicationId":{"type":"keyword"},"jobRunId":{
        |"type":"keyword"},"dataSourceName":{"type":"keyword"},"status":{"type":"keyword"},
        |"error":{"type":"text"}}}
        |""".stripMargin
    val mapping =
      """{"dynamic":false,"properties":{"result":{"type":"object"},"schema":{"type":"object"},
        |"jobRunId":{"type":"keyword"},"applicationId":{
        |"type":"keyword"},"dataSourceName":{"type":"keyword"},"status":{"type":"keyword"}}}
        |"error":{"type":"text"}}}
        |""".stripMargin
    assert(FlintJob.isSuperset(input, mapping))
  }
}
