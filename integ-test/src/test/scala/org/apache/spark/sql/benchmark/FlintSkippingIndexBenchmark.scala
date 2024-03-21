/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.benchmark

import java.util.Locale

import scala.concurrent.duration.DurationInt

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.core.field.bloomfilter.BloomFilterFactory.{ADAPTIVE_NUMBER_CANDIDATE_KEY, BLOOM_FILTER_ADAPTIVE_KEY, CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind._
import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilterSkippingStrategy
import org.opensearch.flint.spark.skipping.minmax.MinMaxSkippingStrategy
import org.opensearch.flint.spark.skipping.partition.PartitionSkippingStrategy
import org.opensearch.flint.spark.skipping.valueset.ValueSetSkippingStrategy
import org.opensearch.flint.spark.skipping.valueset.ValueSetSkippingStrategy.VALUE_SET_MAX_SIZE_KEY

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.Column
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.functions.{expr, rand}

/**
 * Flint skipping index benchmark that focus on skipping data structure read and write performance
 * with OpenSearch based on Spark benchmark framework.
 *
 * Test cases include reading and writing the following skipping data structures:
 * {{{
 *   1. Partition
 *   2. MinMax
 *   3. ValueSet (default size 100)
 *   4. ValueSet (unlimited size)
 *   5. BloomFilter (classic, 1M NDV)
 *   6. BloomFilter (classic, optimal NDV = cardinality)
 *   7. BloomFilter (adaptive, default 10 candidates)
 *   8. BloomFilter (adaptive, 5 candidates)
 *   9. BloomFilter (adaptive, 15 candidates)
 * }}}
 *
 * Test parameters:
 * {{{
 *   1. N = 1M rows of test data generated
 *   2. M = 1 OpenSearch doc written as skipping index
 *   3. cardinality = {64, 2048, 65536} distinct values in N rows
 * }}}
 *
 * To run this benchmark:
 * {{{
 *   > SPARK_GENERATE_BENCHMARK_FILES=1 sbt clean "set every Test / test := {}" "integtest/test:runMain org.apache.spark.sql.benchmark.FlintSkippingIndexBenchmark"
 * }}}
 * Results will be written to "benchmarks/FlintSkippingIndexBenchmark-<JDK>-results.txt".
 */
object FlintSkippingIndexBenchmark extends FlintSparkBenchmark {

  /** How many rows generated in test data */
  private val N = 1000000

  /** How many OpenSearch docs created */
  private val M = 1

  /** Cardinalities of test data */
  private val CARDINALITIES = Seq(64, 2048, 65536)

  /** How many runs for each test case */
  private val WRITE_TEST_NUM_ITERATIONS = 1
  private val READ_TEST_NUM_ITERATIONS = 5

  /** Test index name prefix */
  private val TEST_INDEX_PREFIX = "flint_benchmark"

  /** Test column name and type */
  private val testColName = "value"
  private val testColType = "Int"

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    super.runBenchmarkSuite(mainArgs)
    warmup()

    runBenchmark("Skipping Index Write") {
      CARDINALITIES.foreach(runWriteBenchmark)
    }
    runBenchmark("Skipping Index Read") {
      CARDINALITIES.foreach(runReadBenchmark)
    }
  }

  private def warmup(): Unit = {
    try {
      SkippingKind.values.foreach(kind => {
        writeSkippingIndex(strategy(kind), 1)
        readSkippingIndex(strategy(kind), 1)
      })
    } finally {
      deleteAllTestIndices()
    }
  }

  private def runWriteBenchmark(cardinality: Int): Unit = {
    new Benchmark(
      name = s"Skipping Index Write $N Rows with Cardinality $cardinality",
      valuesPerIteration = N,
      minNumIters = WRITE_TEST_NUM_ITERATIONS,
      warmupTime = (-1).seconds, // ensure no warm up, otherwise test doc duplicate
      minTime = (-1).seconds, // ensure run only once, otherwise test doc duplicate
      output = output)
      .test("Partition Write") { _ =>
        writeSkippingIndex(strategy(PARTITION), cardinality)
      }
      .test("MinMax Write") { _ =>
        writeSkippingIndex(strategy(MIN_MAX), cardinality)
      }
      .test("ValueSet Write (Default Size 100)") { _ =>
        writeSkippingIndex(strategy(VALUE_SET), cardinality)
      }
      .test("ValueSet Write (Unlimited Size)") { _ =>
        writeSkippingIndex(
          strategy(VALUE_SET, Map(VALUE_SET_MAX_SIZE_KEY -> Integer.MAX_VALUE.toString)),
          cardinality)
      }
      .test("BloomFilter Write (1M NDV)") { _ =>
        writeSkippingIndex(
          strategy(
            BLOOM_FILTER,
            Map(
              BLOOM_FILTER_ADAPTIVE_KEY -> "false",
              CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> N.toString)),
          cardinality)
      }
      .test("BloomFilter Write (Optimal NDV)") { _ =>
        writeSkippingIndex(
          strategy(
            BLOOM_FILTER,
            Map(
              BLOOM_FILTER_ADAPTIVE_KEY -> "false",
              CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> cardinality.toString)),
          cardinality)
      }
      .test("Adaptive BloomFilter Write (Default 10 Candidates)") { _ =>
        writeSkippingIndex(strategy(BLOOM_FILTER), cardinality)
      }
      .test("Adaptive BloomFilter Write (5 Candidates)") { _ =>
        writeSkippingIndex(
          strategy(BLOOM_FILTER, params = Map(ADAPTIVE_NUMBER_CANDIDATE_KEY -> "5")),
          cardinality)
      }
      .test("Adaptive BloomFilter Write (15 Candidates)") { _ =>
        writeSkippingIndex(
          strategy(BLOOM_FILTER, params = Map(ADAPTIVE_NUMBER_CANDIDATE_KEY -> "15")),
          cardinality)
      }
      .run()
  }

  private def runReadBenchmark(cardinality: Int): Unit = {
    new Benchmark(
      name = s"Skipping Index Read $N Rows with Cardinality $cardinality",
      valuesPerIteration = M,
      minNumIters = READ_TEST_NUM_ITERATIONS,
      output = output)
      .test("Partition Read") { _ =>
        readSkippingIndex(strategy(PARTITION), cardinality)
      }
      .test("MinMax Read") { _ =>
        readSkippingIndex(strategy(MIN_MAX), cardinality)
      }
      .test("ValueSet Read (Default Size 100)") { _ =>
        readSkippingIndex(strategy(VALUE_SET), cardinality)
      }
      .test("ValueSet Read (Unlimited Size)") { _ =>
        readSkippingIndex(
          strategy(VALUE_SET, Map(VALUE_SET_MAX_SIZE_KEY -> Integer.MAX_VALUE.toString)),
          cardinality)
      }
      .test("BloomFilter Read (1M NDV)") { _ =>
        readSkippingIndex(
          strategy(
            BLOOM_FILTER,
            Map(
              BLOOM_FILTER_ADAPTIVE_KEY -> "false",
              CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> N.toString)),
          cardinality)
      }
      .test("BloomFilter Read (Optimal NDV)") { _ =>
        readSkippingIndex(
          strategy(
            BLOOM_FILTER,
            Map(
              BLOOM_FILTER_ADAPTIVE_KEY -> "false",
              CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> cardinality.toString)),
          cardinality)
      }
      .test("Adaptive BloomFilter Read (Default 10 Candidates)") { _ =>
        readSkippingIndex(strategy(BLOOM_FILTER), cardinality)
      }
      .test("Adaptive BloomFilter Read (5 Candidates)") { _ =>
        readSkippingIndex(
          strategy(BLOOM_FILTER, params = Map(ADAPTIVE_NUMBER_CANDIDATE_KEY -> "5")),
          cardinality)
      }
      .test("Adaptive BloomFilter Read (15 Candidates)") { _ =>
        readSkippingIndex(
          strategy(BLOOM_FILTER, params = Map(ADAPTIVE_NUMBER_CANDIDATE_KEY -> "15")),
          cardinality)
      }
      .run()
  }

  /** Benchmark builder in fluent API style */
  private implicit class BenchmarkBuilder(val benchmark: Benchmark) {

    def test(name: String)(f: Int => Unit): Benchmark = {
      benchmark.addCase(name)(f)
      benchmark
    }
  }

  private def strategy(
      kind: SkippingKind,
      params: Map[String, String] = Map.empty): FlintSparkSkippingStrategy = {
    kind match {
      case PARTITION =>
        PartitionSkippingStrategy(columnName = testColName, columnType = testColType)
      case MIN_MAX => MinMaxSkippingStrategy(columnName = testColName, columnType = testColType)
      case VALUE_SET =>
        ValueSetSkippingStrategy(
          columnName = testColName,
          columnType = testColType,
          params = params)
      case BLOOM_FILTER =>
        BloomFilterSkippingStrategy(
          columnName = testColName,
          columnType = testColType,
          params = params)
    }
  }

  private def readSkippingIndex(indexCol: FlintSparkSkippingStrategy, cardinality: Int): Unit = {
    val schema =
      indexCol.outputSchema().map { case (key, value) => s"$key $value" }.mkString(", ")
    /*
     * Rewrite the query on test column and run it against skipping index created in previous
     * write benchmark.
     */
    val indexQuery =
      indexCol.rewritePredicate(expr(s"$testColName = 1").expr).map(new Column(_)).get
    spark.read
      .format(FLINT_DATASOURCE)
      .options(openSearchOptions)
      .schema(schema)
      .load(getTestIndexName(indexCol, cardinality))
      .where(indexQuery)
      .noop() // Trigger data frame execution and discard output
  }

  private def writeSkippingIndex(indexCol: FlintSparkSkippingStrategy, cardinality: Int): Unit = {
    val testIndexName = getTestIndexName(indexCol, cardinality)

    /*
     * FIXME: must pre-create index because data type lost in bulk JSON and binary code is auto mapped to text by OS
     */
    if (indexCol.kind == BLOOM_FILTER) {
      val mappings =
        s"""{
          |  "properties": {
          |    "$testColName": {
          |      "type": "binary",
          |      "doc_values": true
          |    }
          |  }
          |}""".stripMargin
      val testOSIndexName = testIndexName.toLowerCase(Locale.ROOT)

      openSearchClient.indices.create(
        new CreateIndexRequest(testOSIndexName)
          .mapping(mappings, XContentType.JSON),
        RequestOptions.DEFAULT)
    }

    /*
     * Generate N random numbers with the given cardinality and build single skipping index
     * data structure without group by.
     */
    val namedAggCols = getNamedAggColumn(indexCol)
    spark
      .range(N)
      .withColumn(testColName, (rand() * cardinality + 1).cast(testColType))
      .agg(namedAggCols.head, namedAggCols.tail: _*)
      .coalesce(M)
      .write
      .format(FLINT_DATASOURCE)
      .options(openSearchOptions)
      .mode(Overwrite)
      .save(testIndexName)
  }

  private def getTestIndexName(indexCol: FlintSparkSkippingStrategy, cardinality: Int): String = {
    // Generate unique name as skipping index name in OpenSearch
    val params =
      indexCol.parameters.toSeq
        .sortBy(_._1)
        .map { case (name, value) => s"${name}_$value" }
        .mkString("_")
    val paramsOrDefault = if (params.isEmpty) "default" else params

    s"${TEST_INDEX_PREFIX}_${indexCol.kind}_cardinality_${cardinality}_$paramsOrDefault"
  }

  private def getNamedAggColumn(indexCol: FlintSparkSkippingStrategy): Seq[Column] = {
    val outputNames = indexCol.outputSchema().keys
    val aggFuncs = indexCol.getAggregators

    // Wrap aggregate function with output column name
    (outputNames, aggFuncs).zipped.map { case (name, aggFunc) =>
      new Column(aggFunc.as(name))
    }.toSeq
  }

  private def deleteAllTestIndices(): Unit = {
    openSearchClient
      .indices()
      .delete(new DeleteIndexRequest(s"${TEST_INDEX_PREFIX}_*"), RequestOptions.DEFAULT)
  }
}
