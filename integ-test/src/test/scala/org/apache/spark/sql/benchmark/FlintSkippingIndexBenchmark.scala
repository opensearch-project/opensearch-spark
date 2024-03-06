/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.benchmark

import scala.util.Random

import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName

import org.apache.spark.benchmark.Benchmark

object FlintSkippingIndexBenchmark extends FlintSparkBenchmark {

  private val N = 1000
  private val numFiles = 100

  private val noIndexTableNamePrefix = "spark_catalog.default.no_index_test"
  private val valueSetTableNamePrefix = "spark_catalog.default.value_set_test"
  private val bloomFilterTableNamePrefix = "spark_catalog.default.bloom_filter_test"

  override def afterAll(): Unit = {
    val prefixes =
      Array(noIndexTableNamePrefix, valueSetTableNamePrefix, bloomFilterTableNamePrefix)
    for (prefix <- prefixes) {
      val tables = spark.sql(s"SHOW TABLES IN spark_catalog.default LIKE '$prefix%'").collect()
      for (table <- tables) {
        spark.sql(s"DROP TABLE $table")
      }
    }
    super.afterAll()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    beforeAll()

    runBenchmark("Skipping Index Write") {
      runWriteBenchmark(16)
      runWriteBenchmark(64)
      runWriteBenchmark(512)
    }

    runBenchmark("Skipping Index Read") {
      runReadBenchmark(16)
      runReadBenchmark(64)
      runReadBenchmark(512)
    }
  }

  private def runWriteBenchmark(cardinality: Int): Unit = {
    val benchmark = new Benchmark(
      s"Write $N rows with cardinality $cardinality in $numFiles files",
      numFiles * N,
      output = output)

    // ValueSet write
    val valueSetTableName = s"${valueSetTableNamePrefix}_${N}_$cardinality"
    createTestTable(valueSetTableName, numFiles, N, cardinality)
    flint
      .skippingIndex()
      .onTable(valueSetTableName)
      .addValueSet("col")
      .create()
    benchmark.addCase("ValueSet Write") { _ =>
      flint.refreshIndex(getSkippingIndexName(valueSetTableName))
    }

    // BloomFilter write
    val bloomFilterTableName = s"${bloomFilterTableNamePrefix}_${N}_$cardinality"
    createTestTable(bloomFilterTableName, numFiles, N, cardinality)
    flint
      .skippingIndex()
      .onTable(bloomFilterTableName)
      .addValueSet("col")
      .create()
    benchmark.addCase("BloomFilter Write") { _ =>
      flint.refreshIndex(getSkippingIndexName(bloomFilterTableName))
    }
    benchmark.run()
  }

  private def runReadBenchmark(cardinality: Int): Unit = {
    val benchmark = new Benchmark(
      s"Read $N rows with cardinality $cardinality in $numFiles files",
      numFiles * N,
      output = output)

    val uniqueValue = cardinality + 10
    val noIndexTableName = s"${noIndexTableNamePrefix}_${N}_$cardinality"
    createTestTable(noIndexTableName, numFiles, N, cardinality)
    benchmark.addCase("No Index Read") { _ =>
      spark.sql(s"SELECT * FROM $noIndexTableName WHERE col = $uniqueValue")
    }

    val valueSetTableName = s"${valueSetTableNamePrefix}_${N}_$cardinality"
    // spark.sql(s"INSERT INTO $valueSetTableName VALUES ($uniqueValue)")
    benchmark.addCase("ValueSet Read") { _ =>
      spark.sql(s"SELECT * FROM $valueSetTableName WHERE col = $uniqueValue")
    }

    val bloomFilterTableName = s"${bloomFilterTableNamePrefix}_${N}_$cardinality"
    // spark.sql(s"INSERT INTO $valueSetTableName VALUES ($uniqueValue)")
    benchmark.addCase("BloomFilter Read") { _ =>
      spark.sql(s"SELECT * FROM $bloomFilterTableName WHERE col = $uniqueValue")
    }
    benchmark.run()
  }

  private def createTestTable(
      tableName: String,
      numFiles: Int,
      numRows: Int,
      cardinality: Int): Unit = {
    spark.sql(s"CREATE TABLE $tableName (col INT) USING JSON")

    val uniques = Seq.range(1, cardinality + 1)
    val values =
      Seq
        .fill(numRows)(uniques(Random.nextInt(cardinality)))
        .map(v => s"($v)")
        .mkString(", ")

    for (_ <- 1 to numFiles) {
      spark.sql(s"""
           | INSERT INTO $tableName
           | SELECT /*+ COALESCE(1) */ *
           | FROM VALUES $values
           |""".stripMargin)
    }
  }
}
