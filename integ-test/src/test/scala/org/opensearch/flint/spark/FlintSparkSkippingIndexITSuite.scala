/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.spark.FlintSpark.RefreshMode.{FULL, INCREMENTAL}
import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN
import org.opensearch.flint.spark.skipping.FlintSparkSkippingFileIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.{Matcher, MatchResult}
import org.scalatest.matchers.must.Matchers._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite
import org.apache.spark.sql.{Column, QueryTest, Row}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.flint.config.FlintSparkConf._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkSkippingIndexITSuite
    extends QueryTest
    with FlintSuite
    with OpenSearchSuite
    with StreamTest {

  /** Flint Spark high level API being tested */
  lazy val flint: FlintSpark = {
    setFlintSparkConf(HOST_ENDPOINT, openSearchHost)
    setFlintSparkConf(HOST_PORT, openSearchPort)
    setFlintSparkConf(REFRESH_POLICY, "true")
    new FlintSpark(spark)
  }

  /** Test table and index name */
  private val testTable = "default.test"
  private val testIndex = getSkippingIndexName(testTable)

  override def beforeAll(): Unit = {
    super.beforeAll()

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
        |     "source": "default.test"
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
