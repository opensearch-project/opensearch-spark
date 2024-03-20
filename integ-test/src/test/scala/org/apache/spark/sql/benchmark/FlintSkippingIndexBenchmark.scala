/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.benchmark

import org.opensearch.flint.core.field.bloomfilter.BloomFilterFactory.{ADAPTIVE_NUMBER_CANDIDATE_KEY, BLOOM_FILTER_ADAPTIVE_KEY, CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
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
import org.apache.spark.sql.functions.rand

/**
 * Flint skipping index benchmark that focus on skipping data structure read and write performance
 * with OpenSearch based on Spark benchmark framework.
 *
 * To run this benchmark:
 * {{{
 *   > SPARK_GENERATE_BENCHMARK_FILES=1 sbt clean "set every Test / test := {}" "integtest/test:runMain org.apache.spark.sql.benchmark.FlintSkippingIndexBenchmark"
 *   Results will be written to "benchmarks/FlintSkippingIndexBenchmark-<JDK>-results.txt".
 * }}}
 */
object FlintSkippingIndexBenchmark extends FlintSparkBenchmark {

  /** How many rows generated in test data */
  private val N = 1000000

  /** Test column name and type */
  private val testColName = "value"
  private val testColType = "Int"

  override def runBenchmarkSuite(args: Array[String]): Unit = {
    super.runBenchmarkSuite(args)

    runBenchmark("Skipping Index Write") {
      runWriteBenchmark(64)
      runWriteBenchmark(512)
      runWriteBenchmark(65536)
    }

    /*
    runBenchmark("Skipping Index Read") {
      runReadBenchmark(16)
      runReadBenchmark(64)
      runReadBenchmark(512)
    }
     */
  }

  private def runWriteBenchmark(cardinality: Int): Unit = {
    benchmark(s"Skipping Index Write $N Rows with Cardinality $cardinality")
      .addCase("Partition Write") { _ =>
        buildSkippingIndex(
          PartitionSkippingStrategy(columnName = testColName, columnType = testColType),
          1 // partition column value should be the same in a single file
        )
      }
      .addCase("MinMax Write") { _ =>
        buildSkippingIndex(
          MinMaxSkippingStrategy(columnName = testColName, columnType = testColType),
          cardinality)
      }
      .addCase("ValueSet Write") { _ =>
        buildSkippingIndex(
          ValueSetSkippingStrategy(columnName = testColName, columnType = testColType),
          cardinality)
      }
      .addCase("ValueSet Write (Unlimited Size)") { _ =>
        buildSkippingIndex(
          ValueSetSkippingStrategy(
            columnName = testColName,
            columnType = testColType,
            params = Map(VALUE_SET_MAX_SIZE_KEY -> Integer.MAX_VALUE.toString)),
          cardinality)
      }
      .addCase("BloomFilter Write") { _ =>
        buildSkippingIndex(
          BloomFilterSkippingStrategy(
            columnName = testColName,
            columnType = testColType,
            params = Map(
              BLOOM_FILTER_ADAPTIVE_KEY -> "false",
              CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> N.toString)),
          cardinality)
      }
      .addCase("BloomFilter Write (Optimal NDV)") { _ =>
        buildSkippingIndex(
          BloomFilterSkippingStrategy(
            columnName = testColName,
            columnType = testColType,
            params = Map(
              BLOOM_FILTER_ADAPTIVE_KEY -> "false",
              CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> cardinality.toString)),
          cardinality)
      }
      .addCase("Adaptive BloomFilter Write") { _ =>
        buildSkippingIndex(
          BloomFilterSkippingStrategy(columnName = testColName, columnType = testColType),
          cardinality)
      }
      .addCase("Adaptive BloomFilter Write (5 Candidates)") { _ =>
        buildSkippingIndex(
          BloomFilterSkippingStrategy(
            columnName = testColName,
            columnType = testColType,
            params = Map(ADAPTIVE_NUMBER_CANDIDATE_KEY -> "5")),
          cardinality)
      }
      .addCase("Adaptive BloomFilter Write (15 Candidates)") { _ =>
        buildSkippingIndex(
          BloomFilterSkippingStrategy(
            columnName = testColName,
            columnType = testColType,
            params = Map(ADAPTIVE_NUMBER_CANDIDATE_KEY -> "15")),
          cardinality)
      }
      .run()
  }

  private def runReadBenchmark(cardinality: Int): Unit = {}

  private def benchmark(name: String): BenchmarkBuilder = {
    new BenchmarkBuilder(new Benchmark(name, N, output = output))
  }

  private class BenchmarkBuilder(benchmark: Benchmark) {

    def addCase(name: String)(f: Int => Unit): BenchmarkBuilder = {
      benchmark.addCase(name)(f)
      this
    }

    def run(): Unit = {
      benchmark.run()
    }
  }

  private def buildSkippingIndex(indexCol: FlintSparkSkippingStrategy, cardinality: Int): Unit = {
    val indexName = getTestIndexName(indexCol, cardinality)
    val namedAggCols = getNamedAggColumn(indexCol)

    // Generate N random numbers of cardinality and build single skipping index
    // data structure without grouping by
    spark
      .range(N)
      .withColumn(testColName, (rand() * cardinality + 1).cast(testColType))
      .agg(namedAggCols.head, namedAggCols.tail: _*)
      .write
      .format(FLINT_DATASOURCE)
      .options(openSearchOptions)
      .mode(Overwrite)
      .save(indexName)
  }

  private def getTestIndexName(indexCol: FlintSparkSkippingStrategy, cardinality: Int): String = {
    val params = indexCol.parameters.map { case (name, value) => s"${name}_$value" }.mkString("_")
    s"${indexCol.kind}_${params}_$cardinality"
  }

  private def getNamedAggColumn(indexCol: FlintSparkSkippingStrategy): Seq[Column] = {
    val outputNames = indexCol.outputSchema().keys
    val aggFuncs = indexCol.getAggregators

    // Wrap aggregate function with output column name
    (outputNames, aggFuncs).zipped.map { case (name, aggFunc) =>
      new Column(aggFunc.as(name))
    }.toSeq
  }
}
