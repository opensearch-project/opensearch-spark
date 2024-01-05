/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture}

import scala.concurrent.duration.TimeUnit

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.opensearch.flint.OpenSearchSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite
import org.apache.spark.sql.QueryTest
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

    // Replace executor to avoid impact on IT.
    // TODO: Currently no IT test scheduler so no need to restore it back.
    val mockExecutor = mock[ScheduledExecutorService]
    when(mockExecutor.scheduleWithFixedDelay(any[Runnable], any[Long], any[Long], any[TimeUnit]))
      .thenAnswer((_: InvocationOnMock) => mock[ScheduledFuture[_]])
    FlintSparkIndexMonitor.executor = mockExecutor
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

  protected def createPartitionedMultiRowTable(testTable: String): Unit = {
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

    // Use hint to insert all rows in a single csv file
    sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=4)
           | SELECT /*+ COALESCE(1) */ *
           | FROM VALUES
           |   ('Hello', 20, 'Seattle'),
           |   ('World', 30, 'Portland')
           |""".stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=5)
           | SELECT /*+ COALESCE(1) */ *
           | FROM VALUES
           |   ('Scala', 40, 'Seattle'),
           |   ('Java', 50, 'Portland'),
           |   ('Test', 60, 'Vancouver')
           |""".stripMargin)
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
