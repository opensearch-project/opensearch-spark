/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.nio.file.{Files, Path, Paths}
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
import org.apache.spark.sql.flint.config.FlintSparkConf
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
    // Revoke override in FlintSuite on IT
    conf.unsetConf(FlintSparkConf.CUSTOM_FLINT_SCHEDULER_CLASS.key)

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

  protected def createPeopleTable(testTable: String): Unit = {
    sql(s"""
           | CREATE TABLE $testTable
           | (
           |   id INT,
           |   name STRING,
           |   occupation STRING,
           |   country STRING,
           |   salary INT
           | )
           | USING $tableType $tableOptions
           |""".stripMargin)

    // Insert data into the new table
    sql(s"""
           | INSERT INTO $testTable
           | VALUES (1000, 'Jake', 'Engineer', 'England' , 100000),
           |        (1001, 'Hello', 'Artist', 'USA', 70000),
           |        (1002, 'John', 'Doctor', 'Canada', 120000),
           |        (1003, 'David', 'Doctor', null, 120000),
           |        (1004, 'David', null, 'Canada', 0),
           |        (1005, 'Jane', 'Scientist', 'Canada', 90000)
           | """.stripMargin)
  }

  protected def createWorkInformationTable(testTable: String): Unit = {
    sql(s"""
           | CREATE TABLE $testTable
           | (
           |   uid INT,
           |   name STRING,
           |   department STRING,
           |   occupation STRING
           | )
           | USING $tableType $tableOptions
           |""".stripMargin)

    // Insert data into the new table
    sql(s"""
           | INSERT INTO $testTable
           | VALUES (1000, 'Jake', 'IT', 'Engineer'),
           |        (1002, 'John', 'DATA', 'Scientist'),
           |        (1003, 'David', 'HR', 'Doctor'),
           |        (1005, 'Jane', 'DATA', 'Engineer'),
           |        (1006, 'Tom', 'SALES', 'Artist')
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

  protected def createMultiValueStructTable(testTable: String): Unit = {
    // CSV doesn't support struct field
    sql(s"""
           | CREATE TABLE $testTable
           | (
           |   int_col INT,
           |   multi_value Array<STRUCT<name: STRING, value: INT>>
           | )
           | USING JSON
           |""".stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | SELECT /*+ COALESCE(1) */ *
           | FROM VALUES
           | ( 1, array(STRUCT("1_one", 1), STRUCT(null, 11), STRUCT("1_three", null)) ),
           | ( 2, array(STRUCT("2_Monday", 2), null) ),
           | ( 3, array(STRUCT("3_third", 3), STRUCT("3_4th", 4)) ),
           | ( 4, null )
           |""".stripMargin)
  }

  protected def createMultiColumnArrayTable(testTable: String): Unit = {
    // CSV doesn't support struct field
    sql(s"""
           | CREATE TABLE $testTable
           | (
           |   int_col INT,
           |   multi_valueA Array<STRUCT<name: STRING, value: INT>>,
           |   multi_valueB Array<STRUCT<name: STRING, value: INT>>
           | )
           | USING JSON
           |""".stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | VALUES
           | ( 1, array(STRUCT("1_one", 1), STRUCT(null, 11), STRUCT("1_three", null)),  array(STRUCT("2_Monday", 2), null) ),
           | ( 2, array(STRUCT("2_Monday", 2), null) , array(STRUCT("3_third", 3), STRUCT("3_4th", 4)) ),
           | ( 3, array(STRUCT("3_third", 3), STRUCT("3_4th", 4)) , array(STRUCT("1_one", 1))),
           | ( 4, null, array(STRUCT("1_one", 1)))
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

  protected def createTableHttpLog(testTable: String): Unit = {
    sql(s"""
           | CREATE TABLE $testTable
           |(
           |    id INT,
           |    status_code INT,
           |    request_path STRING,
           |    timestamp STRING
           |)
           | USING $tableType $tableOptions
           |""".stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | VALUES (1, 200, '/home', '2023-10-01 10:00:00'),
           | (2, null, '/about', '2023-10-01 10:05:00'),
           | (3, 500, '/contact', '2023-10-01 10:10:00'),
           | (4, 301, '/home', '2023-10-01 10:15:00'),
           | (5, 200, '/services', '2023-10-01 10:20:00'),
           | (6, 403, '/home', '2023-10-01 10:25:00')
           | """.stripMargin)
  }

  protected def createNullableTableHttpLog(testTable: String): Unit = {
    sql(s"""
           | CREATE TABLE $testTable
           |(
           |    id INT,
           |    status_code INT,
           |    request_path STRING,
           |    timestamp STRING
           |)
           | USING $tableType $tableOptions
           |""".stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | VALUES (1, 200, '/home', null),
           | (2, null, '/about', '2023-10-01 10:05:00'),
           | (3, null, '/contact', '2023-10-01 10:10:00'),
           | (4, 301, null, '2023-10-01 10:15:00'),
           | (5, 200, null, '2023-10-01 10:20:00'),
           | (6, 403, '/home', null)
           | """.stripMargin)
  }

  protected def createNullableJsonContentTable(testTable: String): Unit = {
    sql(s"""
           | CREATE TABLE $testTable
           | (
           |   id INT,
           |   jString STRING,
           |   isValid BOOLEAN
           | )
           | USING $tableType $tableOptions
           |""".stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | VALUES (1, '{"account_number":1,"balance":39225,"age":32,"gender":"M"}', true),
           |        (2, '{"f1":"abc","f2":{"f3":"a","f4":"b"}}', true),
           |        (3, '[1,2,3,{"f1":1,"f2":[5,6]},4]', true),
           |        (4, '[]', true),
           |        (5, '{"teacher":"Alice","student":[{"name":"Bob","rank":1},{"name":"Charlie","rank":2}]}', true),
           |        (6, '[1,2,3]', true),
           |        (7, '[1,2', false),
           |        (8, '[invalid json]', false),
           |        (9, '{"invalid": "json"', false),
           |        (10, 'invalid json', false),
           |        (11, null, false)
           | """.stripMargin)
  }

  protected def createIpAddressTable(testTable: String): Unit = {
    sql(s"""
           | CREATE TABLE $testTable
           | (
           |   id INT,
           |   ipAddress STRING,
           |   isV6 BOOLEAN,
           |   isValid BOOLEAN
           | )
           | USING $tableType $tableOptions
           |""".stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | VALUES (1, '127.0.0.1', false, true),
           |        (2, '192.168.1.0', false, true),
           |        (3, '192.168.1.1', false, true),
           |        (4, '192.168.2.1', false, true),
           |        (5, '192.168.2.', false, false),
           |        (6, '2001:db8::ff00:12:3455', true, true),
           |        (7, '2001:db8::ff00:12:3456', true, true),
           |        (8, '2001:db8::ff00:13:3457', true, true),
           |        (9, '2001:db8::ff00:12:', true, false)
           | """.stripMargin)
  }

  protected def createNestedJsonContentTable(tempFile: Path, testTable: String): Unit = {
    val json =
      """
        |[
        |  {
        |    "_time": "2024-09-13T12:00:00",
        |    "bridges": [
        |      {"name": "Tower Bridge", "length": 801},
        |      {"name": "London Bridge", "length": 928}
        |    ],
        |    "city": "London",
        |    "country": "England",
        |    "coor": {
        |      "lat": 51.5074,
        |      "long": -0.1278,
        |      "alt": 35
        |    }
        |  },
        |  {
        |    "_time": "2024-09-13T12:00:00",
        |    "bridges": [
        |      {"name": "Pont Neuf", "length": 232},
        |      {"name": "Pont Alexandre III", "length": 160}
        |    ],
        |    "city": "Paris",
        |    "country": "France",
        |    "coor": {
        |      "lat": 48.8566,
        |      "long": 2.3522,
        |      "alt": 35
        |    }
        |  },
        |  {
        |    "_time": "2024-09-13T12:00:00",
        |    "bridges": [
        |      {"name": "Rialto Bridge", "length": 48},
        |      {"name": "Bridge of Sighs", "length": 11}
        |    ],
        |    "city": "Venice",
        |    "country": "Italy",
        |    "coor": {
        |      "lat": 45.4408,
        |      "long": 12.3155,
        |      "alt": 2
        |    }
        |  },
        |  {
        |    "_time": "2024-09-13T12:00:00",
        |    "bridges": [
        |      {"name": "Charles Bridge", "length": 516},
        |      {"name": "Legion Bridge", "length": 343}
        |    ],
        |    "city": "Prague",
        |    "country": "Czech Republic",
        |    "coor": {
        |      "lat": 50.0755,
        |      "long": 14.4378,
        |      "alt": 200
        |    }
        |  },
        |  {
        |    "_time": "2024-09-13T12:00:00",
        |    "bridges": [
        |      {"name": "Chain Bridge", "length": 375},
        |      {"name": "Liberty Bridge", "length": 333}
        |    ],
        |    "city": "Budapest",
        |    "country": "Hungary",
        |    "coor": {
        |      "lat": 47.4979,
        |      "long": 19.0402,
        |      "alt": 96
        |    }
        |  },
        |  {
        |    "_time": "1990-09-13T12:00:00",
        |    "bridges": null,
        |    "city": "Warsaw",
        |    "country": "Poland",
        |    "coor": null
        |  }
        |]
        |""".stripMargin
    val tempFile = Files.createTempFile("jsonTestData", ".json")
    val absolutPath = tempFile.toAbsolutePath.toString;
    Files.write(tempFile, json.getBytes)
    sql(s"""
         | CREATE TEMPORARY VIEW $testTable
         | USING org.apache.spark.sql.json
         | OPTIONS (
         |  path "$absolutPath",
         |  multiLine true
         | );
         |""".stripMargin)
  }
}
