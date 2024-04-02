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
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.GetIndexRequest
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

  protected def deleteTestIndex(testIndexNames: String*): Unit = {
    testIndexNames.foreach(testIndex => {

      /**
       * Todo, if state is not valid, will throw IllegalStateException. Should check flint
       * .isRefresh before cleanup resource. Current solution, (1) try to delete flint index, (2)
       * if failed, delete index itself.
       */
      try {
        flint.deleteIndex(testIndex)
        flint.vacuumIndex(testIndex)
      } catch {
        case _: IllegalStateException =>
          if (openSearchClient
              .indices()
              .exists(new GetIndexRequest(testIndex), RequestOptions.DEFAULT)) {
            openSearchClient
              .indices()
              .delete(new DeleteIndexRequest(testIndex), RequestOptions.DEFAULT)
          }
      }
    })
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

  protected def createTableIssue112(testTable: String): Unit = {
    sql(s"""
           | CREATE TABLE $testTable (
           |  resourceSpans ARRAY<STRUCT<
           |      resource: STRUCT<
           |        attributes: ARRAY<STRUCT<key: STRING, value: STRUCT<stringValue: STRING>>>>,
           |  scopeSpans: ARRAY<STRUCT<
           |    scope: STRUCT<name: STRING, version: STRING>,
           |    spans: ARRAY<STRUCT<
           |      attributes: ARRAY<STRUCT<key: STRING, value: STRUCT<intValue: STRING, stringValue: STRING>>>,
           |      endTimeUnixNano: STRING,
           |      kind: BIGINT,
           |      name: STRING,
           |      parentSpanId: STRING,
           |      spanId: STRING,
           |      startTimeUnixNano: STRING,
           |      traceId: STRING>>>>>>)
           |  USING json
           |""".stripMargin)
    sql(s"""INSERT INTO $testTable
           |VALUES (
           |    array(
           |        named_struct(
           |            'resource',
           |            named_struct(
           |                'attributes',
           |                array(
           |                    named_struct('key','telemetry.sdk.version', 'value', named_struct('stringValue', '1.3.0')),
           |                    named_struct('key','telemetry.sdk.name', 'value', named_struct('stringValue', 'opentelemetry'))
           |                )
           |            ),
           |            'scopeSpans',
           |            array(
           |                named_struct(
           |                    'scope',
           |                    named_struct('name', 'opentelemetry_ecto', 'version', '1.1.1'),
           |                    'spans',
           |                    array(
           |                        named_struct(
           |                            'attributes',
           |                            array(
           |                                named_struct('key', 'total_time_microseconds',
           |                                'value', named_struct('stringValue', null, 'intValue',
           |                                '31286')),
           |                                named_struct('key', 'source', 'value', named_struct
           |                                ('stringValue', 'featureflags', 'intValue', null))
           |                            ),
           |                            'endTimeUnixNano', '1698098982202276205',
           |                            'kind', 3,
           |                            'name', 'featureflagservice.repo.query:featureflags',
           |                            'parentSpanId', '9b355ca40dd98f5e',
           |                            'spanId', '87acd6659b425f80',
           |                            'startTimeUnixNano', '1698098982170068232',
           |                            'traceId', 'bc342fb3fbfa54c2188595b89b0b1cd8'
           |                        )
           |                    )
           |                )
           |            )
           |        )
           |    )
           |)""".stripMargin)
  }

  protected def createStructFieldTable(testTable: String): Unit = {
    sql(s"""
         | CREATE TABLE $testTable (
         | id INT,
         | info STRUCT<
         | name: STRING,
         | age: INT>)
         | USING json
         |""".stripMargin)

    sql(s"""
          | INSERT INTO $testTable
          | VALUES
          | (1, STRUCT('Alice', 30)),
          | (2, STRUCT('Bob', 25)),
          | (3, STRUCT('Charlie', 35))
          | """.stripMargin)
  }
}
