/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.nio.file.{Files, Paths}
import java.util.Comparator
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture}

import scala.concurrent.duration.TimeUnit
import scala.util.Try

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.flint.OpenSearchSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.{FlintSuite, SparkConf}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.flint.config.FlintSparkConf.{CHECKPOINT_MANDATORY, HOST_ENDPOINT, HOST_PORT, REFRESH_POLICY}
import org.apache.spark.sql.streaming.StreamTest

/**
 * Flint Spark suite trait that initializes [[FlintSpark]] API instance.
 */
trait FlintSparkSuite extends QueryTest with FlintSuite with OpenSearchSuite with StreamTest {

  /** Flint Spark high level API being tested */
  lazy protected val flint: FlintSpark = new FlintSpark(spark)
  lazy protected val tableType: String = "CSV"
  lazy protected val tableOptions: String = "OPTIONS (header 'false', delimiter '\t')"
  lazy protected val catalogName: String = "spark_catalog"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set(HOST_ENDPOINT.key, openSearchHost)
      .set(HOST_PORT.key, openSearchPort.toString)
      .set(REFRESH_POLICY.key, "true")
      // Disable mandatory checkpoint for test convenience
      .set(CHECKPOINT_MANDATORY.key, "false")
    conf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

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
        // Forcefully delete index data and log entry in case of any errors, such as version conflict
        case _: Exception =>
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

  def deleteDirectory(dirPath: String): Try[Unit] = {
    Try {
      val directory = Paths.get(dirPath)
      Files
        .walk(directory)
        .sorted(Comparator.reverseOrder())
        .forEach(Files.delete(_))
    }
  }

  protected def awaitStreamingComplete(jobId: String): Unit = {
    val job = spark.streams.get(jobId)
    failAfter(streamingTimeout) {
      job.processAllAvailable()
    }
  }

  protected def createPartitionedGrokEmailTable(testTable: String): Unit = {
    spark.sql(s"""
         | CREATE TABLE $testTable
         | (
         |   name STRING,
         |   age INT,
         |   email STRING,
         |   street_address STRING
         | )
         | USING $tableType $tableOptions
         | PARTITIONED BY (
         |    year INT,
         |    month INT
         | )
         |""".stripMargin)

    val data = Seq(
      ("Alice", 30, "alice@example.com", "123 Main St, Seattle", 2023, 4),
      ("Bob", 55, "bob@test.org", "456 Elm St, Portland", 2023, 5),
      ("Charlie", 65, "charlie@domain.net", "789 Pine St, San Francisco", 2023, 4),
      ("David", 19, "david@anotherdomain.com", "101 Maple St, New York", 2023, 5),
      ("Eve", 21, "eve@examples.com", "202 Oak St, Boston", 2023, 4),
      ("Frank", 76, "frank@sample.org", "303 Cedar St, Austin", 2023, 5),
      ("Grace", 41, "grace@demo.net", "404 Birch St, Chicago", 2023, 4),
      ("Hank", 32, "hank@demonstration.com", "505 Spruce St, Miami", 2023, 5),
      ("Ivy", 9, "ivy@examples.com", "606 Fir St, Denver", 2023, 4),
      ("Jack", 12, "jack@sample.net", "707 Ash St, Seattle", 2023, 5))

    data.foreach { case (name, age, email, street_address, year, month) =>
      spark.sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=$year, month=$month)
           | VALUES ('$name', $age, '$email', '$street_address')
           | """.stripMargin)
    }
  }
  protected def createPartitionedAddressTable(testTable: String): Unit = {
    sql(s"""
         | CREATE TABLE $testTable
         | (
         |   name STRING,
         |   age INT,
         |   address STRING
         | )
         | USING $tableType $tableOptions
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

  protected def createPartitionedMultiRowAddressTable(testTable: String): Unit = {
    sql(s"""
        | CREATE TABLE $testTable
        | (
        |   name STRING,
        |   age INT,
        |   address STRING
        | )
        | USING $tableType $tableOptions
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

  protected def createPartitionedStateCountryTable(testTable: String): Unit = {
    sql(s"""
          | CREATE TABLE $testTable
          | (
          |   name STRING,
          |   age INT,
          |   state STRING,
          |   country STRING
          | )
          | USING $tableType $tableOptions
          | PARTITIONED BY (
          |    year INT,
          |    month INT
          | )
          |""".stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=4)
           | VALUES ('Jake', 70, 'California', 'USA'),
           |        ('Hello', 30, 'New York', 'USA'),
           |        ('John', 25, 'Ontario', 'Canada'),
           |        ('Jane', 20, 'Quebec', 'Canada')
           | """.stripMargin)
  }

  protected def createNullableStateCountryTable(testTable: String): Unit = {
    sql(s"""
           | CREATE TABLE $testTable
           | (
           |   name STRING,
           |   age INT,
           |   state STRING,
           |   country STRING
           | )
           | USING $tableType $tableOptions
           |""".stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | VALUES ('Jake', 70, 'California', 'USA'),
           |        ('Hello', 30, 'New York', 'USA'),
           |        ('John', 25, 'Ontario', 'Canada'),
           |        ('Jane', 20, 'Quebec', 'Canada'),
           |        (null, 10, null, 'Canada')
           | """.stripMargin)
  }

  protected def createOccupationTable(testTable: String): Unit = {
    sql(s"""
      | CREATE TABLE $testTable
      | (
      |   name STRING,
      |   occupation STRING,
      |   country STRING,
      |   salary INT
      | )
      | USING $tableType $tableOptions
      | PARTITIONED BY (
      |    year INT,
      |    month INT
      | )
      |""".stripMargin)

    // Insert data into the new table
    sql(s"""
         | INSERT INTO $testTable
         | PARTITION (year=2023, month=4)
         | VALUES ('Jake', 'Engineer', 'England' , 100000),
         |        ('Hello', 'Artist', 'USA', 70000),
         |        ('John', 'Doctor', 'Canada', 120000),
         |        ('David', 'Doctor', 'USA', 120000),
         |        ('David', 'Unemployed', 'Canada', 0),
         |        ('Jane', 'Scientist', 'Canada', 90000)
         | """.stripMargin)
  }

  protected def createOccupationTopRareTable(testTable: String): Unit = {
    sql(s"""
      | CREATE TABLE $testTable
      | (
      |   name STRING,
      |   occupation STRING,
      |   country STRING,
      |   salary INT
      | )
      | USING $tableType $tableOptions
      | PARTITIONED BY (
      |    year INT,
      |    month INT
      | )
      |""".stripMargin)

    // Insert data into the new table
    sql(s"""
         | INSERT INTO $testTable
         | PARTITION (year=2023, month=4)
         | VALUES ('Jake', 'Engineer', 'England' , 100000),
         |        ('Hello', 'Artist', 'USA', 70000),
         |        ('John', 'Doctor', 'Canada', 120000),
         |        ('Rachel', 'Doctor', 'Canada', 220000),
         |        ('Henry', 'Doctor', 'Canada', 220000),
         |        ('David', 'Engineer', 'USA', 320000),
         |        ('Barty', 'Engineer', 'USA', 120000),
         |        ('David', 'Unemployed', 'Canada', 0),
         |        ('Jane', 'Scientist', 'Canada', 90000),
         |        ('Philip', 'Scientist', 'Canada', 190000)
         | """.stripMargin)
  }

  protected def createHobbiesTable(testTable: String): Unit = {
    sql(s"""
        | CREATE TABLE $testTable
        | (
        |   name STRING,
        |   country STRING,
        |   hobby STRING,
        |   language STRING
        | )
        | USING $tableType $tableOptions
        | PARTITIONED BY (
        |    year INT,
        |    month INT
        | )
        |""".stripMargin)

    // Insert data into the new table
    sql(s"""
      | INSERT INTO $testTable
      | PARTITION (year=2023, month=4)
      | VALUES ('Jake', 'USA', 'Fishing', 'English'),
      |        ('Hello', 'USA', 'Painting', 'English'),
      |        ('John', 'Canada', 'Reading', 'French'),
      |        ('Jim', 'Canada', 'Hiking', 'English'),
      |        ('Peter', 'Canada', 'Gaming', 'English'),
      |        ('Rick', 'USA', 'Swimming', 'English'),
      |        ('David', 'USA', 'Gardening', 'English'),
      |        ('Jane', 'Canada', 'Singing', 'French')
      | """.stripMargin)
  }

  protected def createDuplicationNullableTable(testTable: String): Unit = {
    sql(s"""
           | CREATE TABLE $testTable
           | (
           |   id INT,
           |   name STRING,
           |   category STRING
           | )
           | USING $tableType $tableOptions
           |""".stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | VALUES   (1, "A", "X"),
           |          (2, "A", "Y"),
           |          (3, "A", "Y"),
           |          (4, "B", "Z"),
           |          (5, "B", "Z"),
           |          (6, "B", "Z"),
           |          (7, "C", "X"),
           |          (8, null, "Y"),
           |          (9, "D", "Z"),
           |          (10, "E", null),
           |          (11, "A", "X"),
           |          (12, "A", "Y"),
           |          (13, null, "X"),
           |          (14, "B", null),
           |          (15, "B", "Y"),
           |          (16, null, "Z"),
           |          (17, "C", "X"),
           |          (18, null, null)
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
      | USING $tableType $tableOptions
      |""".stripMargin)

    sql(s"INSERT INTO $testTable VALUES (TIMESTAMP '2023-10-01 00:01:00', 'A', 30, 'Seattle')")
    sql(s"INSERT INTO $testTable VALUES (TIMESTAMP '2023-10-01 00:10:00', 'B', 20, 'Seattle')")
    sql(s"INSERT INTO $testTable VALUES (TIMESTAMP '2023-10-01 00:15:00', 'C', 35, 'Portland')")
    sql(s"INSERT INTO $testTable VALUES (TIMESTAMP '2023-10-01 01:00:00', 'D', 40, 'Portland')")
    sql(s"INSERT INTO $testTable VALUES (TIMESTAMP '2023-10-01 03:00:00', 'E', 15, 'Vancouver')")
  }

  protected def createTimeSeriesTransactionTable(testTable: String): Unit = {
    sql(s"""
      | CREATE TABLE $testTable
      | (
      |   transactionId STRING,
      |   transactionDate TIMESTAMP,
      |   productId STRING,
      |   productsAmount INT,
      |   customerId STRING
      | )
      | USING $tableType $tableOptions
      | PARTITIONED BY (
      |    year INT,
      |    month INT
      | )
      |""".stripMargin)

    // Update data insertion
    // -- Inserting records into the testTable for April 2023
    sql(s"""
      | INSERT INTO $testTable PARTITION (year=2023, month=4)
      | VALUES
      | ('txn001', CAST('2023-04-01 10:30:00' AS TIMESTAMP), 'prod1', 2, 'cust1'),
      | ('txn001', CAST('2023-04-01 14:30:00' AS TIMESTAMP), 'prod1', 4, 'cust1'),
      | ('txn002', CAST('2023-04-02 11:45:00' AS TIMESTAMP), 'prod2', 1, 'cust2'),
      | ('txn003', CAST('2023-04-03 12:15:00' AS TIMESTAMP), 'prod3', 3, 'cust1'),
      | ('txn004', CAST('2023-04-04 09:50:00' AS TIMESTAMP), 'prod1', 1, 'cust3')
      |  """.stripMargin)

    // Update data insertion
    // -- Inserting records into the testTable for May 2023
    sql(s"""
      | INSERT INTO $testTable PARTITION (year=2023, month=5)
      | VALUES
      | ('txn005', CAST('2023-05-01 08:30:00' AS TIMESTAMP), 'prod2', 1, 'cust4'),
      | ('txn006', CAST('2023-05-02 07:25:00' AS TIMESTAMP), 'prod4', 5, 'cust2'),
      | ('txn007', CAST('2023-05-03 15:40:00' AS TIMESTAMP), 'prod3', 1, 'cust3'),
      | ('txn007', CAST('2023-05-03 19:30:00' AS TIMESTAMP), 'prod3', 2, 'cust3'),
      | ('txn008', CAST('2023-05-04 14:15:00' AS TIMESTAMP), 'prod1', 4, 'cust1')
      |  """.stripMargin)
  }

  protected def createStructTable(testTable: String): Unit = {
    // CSV doesn't support struct field
    sql(s"""
         | CREATE TABLE $testTable
         | (
         |   int_col INT,
         |   struct_col STRUCT<field1: STRUCT<subfield:STRING>, field2: INT>
         | )
         | USING JSON
         |""".stripMargin)

    sql(s"""
         | INSERT INTO $testTable
         | SELECT /*+ COALESCE(1) */ *
         | FROM VALUES
         | ( 30, STRUCT(STRUCT("value1"),123) ),
         | ( 40, STRUCT(STRUCT("value2"),456) )
         |""".stripMargin)
    sql(s"""
         | INSERT INTO $testTable
         | VALUES ( 50, STRUCT(STRUCT("value3"),789) )
         |""".stripMargin)
  }

  protected def createStructNestedTable(testTable: String): Unit = {
    sql(s"""
         | CREATE TABLE $testTable
         | (
         |   int_col INT,
         |   struct_col  STRUCT<field1: STRUCT<subfield:STRING>, field2: INT>,
         |   struct_col2 STRUCT<field1: STRUCT<subfield:STRING>, field2: INT>
         | )
         | USING JSON
         |""".stripMargin)

    sql(s"""
         | INSERT INTO $testTable
         | SELECT /*+ COALESCE(1) */ *
         | FROM VALUES
         | ( 30, STRUCT(STRUCT("value1"),123), STRUCT(STRUCT("valueA"),23) ),
         | ( 40, STRUCT(STRUCT("value5"),123), STRUCT(STRUCT("valueB"),33) ),
         | ( 30, STRUCT(STRUCT("value4"),823), STRUCT(STRUCT("valueC"),83) ),
         | ( 40, STRUCT(STRUCT("value2"),456), STRUCT(STRUCT("valueD"),46) )
         |""".stripMargin)
    sql(s"""
         | INSERT INTO $testTable
         | VALUES ( 50, STRUCT(STRUCT("value3"),789), STRUCT(STRUCT("valueE"),89) )
         |""".stripMargin)
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
}
