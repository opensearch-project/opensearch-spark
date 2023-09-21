/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.core.FlintVersion.current
import org.opensearch.flint.spark.FlintSpark.RefreshMode.{FULL, INCREMENTAL}
import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN
import org.opensearch.flint.spark.skipping.FlintSparkSkippingFileIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.{Matcher, MatchResult}
import org.scalatest.matchers.must.Matchers._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.flint.config.FlintSparkConf._
import org.apache.spark.sql.functions.col

class FlintSparkSkippingIndexITSuite extends FlintSparkSuite {

  /** Test table and index name */
  private val testTable = "default.test"
  private val testIndex = getSkippingIndexName(testTable)

  override def beforeAll(): Unit = {
    super.beforeAll()

    createPartitionedTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()

    // Delete all test indices
    flint.deleteIndex(testIndex)
  }

  test("create skipping index with metadata successfully") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .addValueSet("address")
      .addMinMax("age")
      .create()

    val indexName = s"flint_default_test_skipping_index"
    val index = flint.describeIndex(indexName)
    index shouldBe defined
    index.get.metadata().getContent should matchJson(s"""{
        |   "_meta": {
        |     "name": "flint_default_test_skipping_index",
        |     "version": "${current()}",
        |     "kind": "skipping",
        |     "indexedColumns": [
        |     {
        |        "kind": "PARTITION",
        |        "columnName": "year",
        |        "columnType": "int"
        |     },
        |     {
        |        "kind": "PARTITION",
        |        "columnName": "month",
        |        "columnType": "int"
        |     },
        |     {
        |        "kind": "VALUE_SET",
        |        "columnName": "address",
        |        "columnType": "string"
        |     },
        |     {
        |        "kind": "MIN_MAX",
        |        "columnName": "age",
        |        "columnType": "int"
        |     }],
        |     "source": "default.test",
        |     "options": {}
        |   },
        |   "properties": {
        |     "year": {
        |       "type": "integer"
        |     },
        |     "month": {
        |       "type": "integer"
        |     },
        |     "address": {
        |       "type": "keyword"
        |     },
        |     "MinMax_age_0": {
        |       "type": "integer"
        |     },
        |     "MinMax_age_1" : {
        |       "type": "integer"
        |     },
        |     "file_path": {
        |       "type": "keyword"
        |     }
        |   }
        | }
        |""".stripMargin)

    index.get.options shouldBe FlintSparkIndexOptions.empty
  }

  test("create skipping index with index options successfully") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addValueSet("address")
      .options(FlintSparkIndexOptions(Map(
        "auto_refresh" -> "true",
        "refresh_interval" -> "1 Minute",
        "checkpoint_location" -> "s3a://test/"
      )))
      .create()

    val index = flint.describeIndex(testIndex)
    index shouldBe defined
    index.get.options.autoRefresh() shouldBe true
    index.get.options.refreshInterval() shouldBe Some("1 Minute")
    index.get.options.checkpointLocation() shouldBe Some("s3a://test/")
  }

  test("should not have ID column in index data") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year")
      .create()
    flint.refreshIndex(testIndex, FULL)

    val indexData = flint.queryIndex(testIndex)
    indexData.columns should not contain ID_COLUMN
  }

  test("full refresh skipping index successfully") {
    // Create Flint index and wait for complete
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()

    val jobId = flint.refreshIndex(testIndex, FULL)
    jobId shouldBe empty

    val indexData = flint.queryIndex(testIndex).collect().toSet
    indexData should have size 2
  }

  test("incremental refresh skipping index successfully") {
    // Create Flint index and wait for complete
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()

    val jobId = flint.refreshIndex(testIndex, INCREMENTAL)
    jobId shouldBe defined

    val job = spark.streams.get(jobId.get)
    failAfter(streamingTimeout) {
      job.processAllAvailable()
    }

    val indexData = flint.queryIndex(testIndex).collect().toSet
    indexData should have size 2
  }

  test("should fail to manual refresh an incremental refreshing index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()

    val jobId = flint.refreshIndex(testIndex, INCREMENTAL)
    val job = spark.streams.get(jobId.get)
    failAfter(streamingTimeout) {
      job.processAllAvailable()
    }

    assertThrows[IllegalStateException] {
      flint.refreshIndex(testIndex, FULL)
    }
  }

  test("can have only 1 skipping index on a table") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year")
      .create()

    assertThrows[IllegalStateException] {
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year")
        .create()
    }
  }

  test("can have only 1 skipping strategy on a column") {
    assertThrows[IllegalArgumentException] {
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year")
        .addValueSet("year")
        .create()
    }
  }

  test("should not rewrite original query if no skipping index") {
    val query =
      s"""
         | SELECT name
         | FROM $testTable
         | WHERE year = 2023 AND month = 4
         |""".stripMargin

    val actual = sql(query).queryExecution.optimizedPlan
    withFlintOptimizerDisabled {
      val expect = sql(query).queryExecution.optimizedPlan
      actual shouldBe expect
    }
  }

  test("can build partition skipping index and rewrite applicable query") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()
    flint.refreshIndex(testIndex, FULL)

    val query = sql(s"""
                       | SELECT name
                       | FROM $testTable
                       | WHERE year = 2023 AND month = 4
                       |""".stripMargin)

    checkAnswer(query, Row("Hello"))
    query.queryExecution.executedPlan should
      useFlintSparkSkippingFileIndex(hasIndexFilter(col("year") === 2023 && col("month") === 4))
  }

  test("can build value set skipping index and rewrite applicable query") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addValueSet("address")
      .create()
    flint.refreshIndex(testIndex, FULL)

    val query = sql(s"""
                       | SELECT name
                       | FROM $testTable
                       | WHERE address = 'Portland'
                       |""".stripMargin)

    checkAnswer(query, Row("World"))
    query.queryExecution.executedPlan should
      useFlintSparkSkippingFileIndex(hasIndexFilter(col("address") === "Portland"))
  }

  test("can build min max skipping index and rewrite applicable query") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addMinMax("age")
      .create()
    flint.refreshIndex(testIndex, FULL)

    val query = sql(s"""
                       | SELECT name
                       | FROM $testTable
                       | WHERE age = 25
                       |""".stripMargin)

    checkAnswer(query, Row("World"))
    query.queryExecution.executedPlan should
      useFlintSparkSkippingFileIndex(
        hasIndexFilter(col("MinMax_age_0") <= 25 && col("MinMax_age_1") >= 25))
  }

  test("should rewrite applicable query with table name without database specified") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year")
      .create()

    // Table name without database name "default"
    val query = sql(s"""
                       | SELECT name
                       | FROM test
                       | WHERE year = 2023
                       |""".stripMargin)

    query.queryExecution.executedPlan should
      useFlintSparkSkippingFileIndex(
        hasIndexFilter(col("year") === 2023))
  }

  test("should not rewrite original query if filtering condition has disjunction") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .addValueSet("address")
      .create()

    val queries = Seq(
      s"""
         | SELECT name
         | FROM $testTable
         | WHERE year = 2023 OR month = 4
         |""".stripMargin,
      s"""
         | SELECT name
         | FROM $testTable
         | WHERE year = 2023 AND (month = 4 OR address = 'Seattle')
         |""".stripMargin)

    for (query <- queries) {
      val actual = sql(query).queryExecution.optimizedPlan
      withFlintOptimizerDisabled {
        val expect = sql(query).queryExecution.optimizedPlan
        actual shouldBe expect
      }
    }
  }

  test("should rewrite applicable query to scan latest source files in hybrid scan mode") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("month")
      .create()
    flint.refreshIndex(testIndex, FULL)

    // Generate a new source file which is not in index data
    sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=4)
           | VALUES ('Hello', 35, 'Vancouver')
           | """.stripMargin)

    withHybridScanEnabled {
      val query = sql(s"""
                         | SELECT address
                         | FROM $testTable
                         | WHERE month = 4
                         |""".stripMargin)

      checkAnswer(query, Seq(Row("Seattle"), Row("Vancouver")))
    }
  }

  test("should return empty if describe index not exist") {
    flint.describeIndex("non-exist") shouldBe empty
  }

  test("create skipping index for all supported data types successfully") {
    // Prepare test table
    val testTable = "default.data_type_table"
    val testIndex = getSkippingIndexName(testTable)
    sql(
      s"""
         | CREATE TABLE $testTable
         | (
         |   boolean_col BOOLEAN,
         |   string_col STRING,
         |   varchar_col VARCHAR(20),
         |   char_col CHAR(20),
         |   long_col LONG,
         |   int_col INT,
         |   short_col SHORT,
         |   byte_col BYTE,
         |   double_col DOUBLE,
         |   float_col FLOAT,
         |   timestamp_col TIMESTAMP,
         |   date_col DATE,
         |   struct_col STRUCT<subfield1: STRING, subfield2: INT>
         | )
         | USING PARQUET
         |""".stripMargin)
    sql(
      s"""
         | INSERT INTO $testTable
         | VALUES (
         |   TRUE,
         |   "sample string",
         |   "sample varchar",
         |   "sample char",
         |   1L,
         |   2,
         |   3S,
         |   4Y,
         |   5.0D,
         |   6.0F,
         |   TIMESTAMP "2023-08-09 17:24:40.322171",
         |   DATE "2023-08-09",
         |   STRUCT("subfieldValue1",123)
         | )
         |""".stripMargin)

    // Create index on all columns
    flint
      .skippingIndex()
      .onTable(testTable)
      .addValueSet("boolean_col")
      .addValueSet("string_col")
      .addValueSet("varchar_col")
      .addValueSet("char_col")
      .addValueSet("long_col")
      .addValueSet("int_col")
      .addValueSet("short_col")
      .addValueSet("byte_col")
      .addValueSet("double_col")
      .addValueSet("float_col")
      .addValueSet("timestamp_col")
      .addValueSet("date_col")
      .addValueSet("struct_col")
      .create()

    val index = flint.describeIndex(testIndex)
    index shouldBe defined
    index.get.metadata().getContent should matchJson(
      s"""{
         |   "_meta": {
         |     "name": "flint_default_data_type_table_skipping_index",
         |     "version": "${current()}",
         |     "kind": "skipping",
         |     "indexedColumns": [
         |     {
         |        "kind": "VALUE_SET",
         |        "columnName": "boolean_col",
         |        "columnType": "boolean"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "columnName": "string_col",
         |        "columnType": "string"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "columnName": "varchar_col",
         |        "columnType": "varchar(20)"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "columnName": "char_col",
         |        "columnType": "char(20)"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "columnName": "long_col",
         |        "columnType": "bigint"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "columnName": "int_col",
         |        "columnType": "int"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "columnName": "short_col",
         |        "columnType": "smallint"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "columnName": "byte_col",
         |        "columnType": "tinyint"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "columnName": "double_col",
         |        "columnType": "double"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "columnName": "float_col",
         |        "columnType": "float"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "columnName": "timestamp_col",
         |        "columnType": "timestamp"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "columnName": "date_col",
         |        "columnType": "date"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "columnName": "struct_col",
         |        "columnType": "struct<subfield1:string,subfield2:int>"
         |     }],
         |     "source": "$testTable",
         |     "options": {}
         |   },
         |   "properties": {
         |     "boolean_col": {
         |       "type": "boolean"
         |     },
         |     "string_col": {
         |       "type": "keyword"
         |     },
         |     "varchar_col": {
         |       "type": "keyword"
         |     },
         |     "char_col": {
         |       "type": "keyword"
         |     },
         |     "long_col": {
         |       "type": "long"
         |     },
         |     "int_col": {
         |       "type": "integer"
         |     },
         |     "short_col": {
         |       "type": "short"
         |     },
         |     "byte_col": {
         |       "type": "byte"
         |     },
         |     "double_col": {
         |       "type": "double"
         |     },
         |     "float_col": {
         |       "type": "float"
         |     },
         |     "timestamp_col": {
         |       "type": "date",
         |       "format": "strict_date_optional_time_nanos"
         |     },
         |     "date_col": {
         |       "type": "date",
         |       "format": "strict_date"
         |     },
         |     "struct_col": {
         |       "properties": {
         |         "subfield1": {
         |           "type": "keyword"
         |         },
         |         "subfield2": {
         |           "type": "integer"
         |         }
         |       }
         |     },
         |     "file_path": {
         |       "type": "keyword"
         |     }
         |   }
         | }
         |""".stripMargin)

    flint.deleteIndex(testIndex)
  }

  test("can build skipping index for varchar and char and rewrite applicable query") {
    val testTable = "default.varchar_char_table"
    val testIndex = getSkippingIndexName(testTable)
    sql(
      s"""
         | CREATE TABLE $testTable
         | (
         |   varchar_col VARCHAR(20),
         |   char_col CHAR(20)
         | )
         | USING PARQUET
         |""".stripMargin)
    sql(
      s"""
         | INSERT INTO $testTable
         | VALUES (
         |   "sample varchar",
         |   "sample char"
         | )
         |""".stripMargin)

    flint
      .skippingIndex()
      .onTable(testTable)
      .addValueSet("varchar_col")
      .addValueSet("char_col")
      .create()
    flint.refreshIndex(testIndex, FULL)

    val query = sql(
      s"""
         | SELECT varchar_col, char_col
         | FROM $testTable
         | WHERE varchar_col = "sample varchar" AND char_col = "sample char"
         |""".stripMargin)

    // CharType column is padded to a fixed length with whitespace
    val paddedChar = "sample char".padTo(20, ' ')
    checkAnswer(query, Row("sample varchar", paddedChar))
    query.queryExecution.executedPlan should
      useFlintSparkSkippingFileIndex(hasIndexFilter(
        col("varchar_col") === "sample varchar" && col("char_col") === paddedChar))

    flint.deleteIndex(testIndex)
  }

  // Custom matcher to check if a SparkPlan uses FlintSparkSkippingFileIndex
  def useFlintSparkSkippingFileIndex(
      subMatcher: Matcher[FlintSparkSkippingFileIndex]): Matcher[SparkPlan] = {
    Matcher { (plan: SparkPlan) =>
      val useFlintSparkSkippingFileIndex = plan.collect {
        case FileSourceScanExec(
              HadoopFsRelation(fileIndex: FlintSparkSkippingFileIndex, _, _, _, _, _),
              _,
              _,
              _,
              _,
              _,
              _,
              _,
              _) =>
          fileIndex should subMatcher
      }.nonEmpty

      MatchResult(
        useFlintSparkSkippingFileIndex,
        "Plan does not use FlintSparkSkippingFileIndex",
        "Plan uses FlintSparkSkippingFileIndex")
    }
  }

  // Custom matcher to check if FlintSparkSkippingFileIndex has expected filter condition
  def hasIndexFilter(expect: Column): Matcher[FlintSparkSkippingFileIndex] = {
    Matcher { (fileIndex: FlintSparkSkippingFileIndex) =>
      val hasExpectedFilter = fileIndex.indexFilter.semanticEquals(expect.expr)

      MatchResult(
        hasExpectedFilter,
        "FlintSparkSkippingFileIndex does not have expected filter",
        "FlintSparkSkippingFileIndex has expected filter")
    }
  }

  private def withFlintOptimizerDisabled(block: => Unit): Unit = {
    spark.conf.set(OPTIMIZER_RULE_ENABLED.key, "false")
    try {
      block
    } finally {
      spark.conf.set(OPTIMIZER_RULE_ENABLED.key, "true")
    }
  }
}
