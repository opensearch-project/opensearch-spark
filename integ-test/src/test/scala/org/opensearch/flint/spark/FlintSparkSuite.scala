/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.OpenSearchSuite
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf.{CHECKPOINT_MANDATORY, HOST_ENDPOINT, HOST_PORT, REFRESH_POLICY}
import org.apache.spark.sql.streaming.StreamTest

/**
 * Flint Spark suite trait that initializes [[FlintSpark]] API instance.
 */
trait FlintSparkSuite extends QueryTest with FlintSuite with OpenSearchSuite with StreamTest {

  /** Flint Spark high level API being tested */
  lazy protected val flint: FlintSpark = new FlintSpark(spark)

  override def beforeAll(): Unit = {
    super.beforeAll()

    setFlintSparkConf(HOST_ENDPOINT, openSearchHost)
    setFlintSparkConf(HOST_PORT, openSearchPort)
    setFlintSparkConf(REFRESH_POLICY, "true")

    // Disable mandatory checkpoint for test convenience
    setFlintSparkConf(CHECKPOINT_MANDATORY, "false")
  }

  protected def awaitStreamingDataComplete(flintIndexName: String): DataFrame = {
    val job = spark.streams.active.find(_.name == flintIndexName)
    job shouldBe defined

    failAfter(streamingTimeout) {
      job.get.processAllAvailable()
    }
    spark.read.format(FLINT_DATASOURCE).load(flintIndexName)
  }

  protected def awaitStreamingComplete(jobId: String): Unit = {
    val job = spark.streams.get(jobId)
    failAfter(streamingTimeout) {
      job.processAllAvailable()
    }
  }

  protected def createPartitionedTable(testTable: String): Unit = {
    sql(s"""
         | CREATE TABLE $testTable
         | (
         |   name STRING,
         |   age INT,
         |   address STRING
         | )
         | USING CSV
         | OPTIONS (
         |  header 'false',
         |  delimiter '\t'
         | )
         | PARTITIONED BY (
         |    year INT,
         |    month INT
         | )
         |""".stripMargin)

    sql(s"""
         | INSERT INTO $testTable
         | PARTITION (year=2023, month=4)
         | VALUES ('Hello', 30, 'Seattle')
         | """.stripMargin)

    sql(s"""
         | INSERT INTO $testTable
         | PARTITION (year=2023, month=5)
         | VALUES ('World', 25, 'Portland')
         | """.stripMargin)
  }

  protected def createTimeSeriesTable(testTable: String): Unit = {
    sql(s"""
         | CREATE TABLE $testTable
         | (
         |   time TIMESTAMP,
         |   name STRING,
         |   age INT,
         |   address STRING
         | )
         | USING CSV
         | OPTIONS (
         |  header 'false',
         |  delimiter '\t'
         | )
         |""".stripMargin)

    sql(s"INSERT INTO $testTable VALUES (TIMESTAMP '2023-10-01 00:01:00', 'A', 30, 'Seattle')")
    sql(s"INSERT INTO $testTable VALUES (TIMESTAMP '2023-10-01 00:10:00', 'B', 20, 'Seattle')")
    sql(s"INSERT INTO $testTable VALUES (TIMESTAMP '2023-10-01 00:15:00', 'C', 35, 'Portland')")
    sql(s"INSERT INTO $testTable VALUES (TIMESTAMP '2023-10-01 01:00:00', 'D', 40, 'Portland')")
    sql(s"INSERT INTO $testTable VALUES (TIMESTAMP '2023-10-01 03:00:00', 'E', 15, 'Vancouver')")
  }
}
