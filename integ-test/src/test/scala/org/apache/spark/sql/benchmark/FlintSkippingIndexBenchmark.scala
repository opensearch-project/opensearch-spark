/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.benchmark

import org.opensearch.flint.core.field.bloomfilter.BloomFilterFactory.{ADAPTIVE_NUMBER_CANDIDATE_KEY, BLOOM_FILTER_ADAPTIVE_KEY, CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
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
 * To run this benchmark:
 * {{{
 *   > SPARK_GENERATE_BENCHMARK_FILES=1 sbt clean "set every Test / test := {}" "integtest/test:runMain org.apache.spark.sql.benchmark.FlintSkippingIndexBenchmark"
 * }}}
 * Results will be written to "benchmarks/FlintSkippingIndexBenchmark-<JDK>-results.txt".
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
    runBenchmark("Skipping Index Read") {
      runReadBenchmark(16)
      runReadBenchmark(512)
      runReadBenchmark(65536)
    }
  }

  private def runWriteBenchmark(cardinality: Int): Unit = {
    benchmark(s"Skipping Index Write $N Rows with Cardinality $cardinality", N)
      .addCase("Partition Write") { _ =>
        // Partitioned column cardinality must be 1 (all values are the same in a single file0
        writeSkippingIndex(strategy(PARTITION), 1)
      }
      .addCase("MinMax Write") { _ =>
        writeSkippingIndex(strategy(MIN_MAX), cardinality)
      }
      .addCase("ValueSet Write (Default Size 100)") { _ =>
        writeSkippingIndex(strategy(VALUE_SET), cardinality)
      }
      .addCase("ValueSet Write (Unlimited Size)") { _ =>
        writeSkippingIndex(
          strategy(VALUE_SET, Map(VALUE_SET_MAX_SIZE_KEY -> Integer.MAX_VALUE.toString)),
          cardinality)
      }
      .addCase("BloomFilter Write (1M NDV)") { _ =>
        writeSkippingIndex(
          strategy(
            BLOOM_FILTER,
            Map(
              BLOOM_FILTER_ADAPTIVE_KEY -> "false",
              CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> N.toString)),
          cardinality)
      }
      .addCase("BloomFilter Write (Optimal NDV)") { _ =>
        writeSkippingIndex(
          strategy(
            BLOOM_FILTER,
            Map(
              BLOOM_FILTER_ADAPTIVE_KEY -> "false",
              CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> cardinality.toString)),
          cardinality)
      }
      .addCase("Adaptive BloomFilter Write (Default 10 Candidates)") { _ =>
        writeSkippingIndex(strategy(BLOOM_FILTER), cardinality)
      }
      .addCase("Adaptive BloomFilter Write (5 Candidates)") { _ =>
        writeSkippingIndex(
          strategy(BLOOM_FILTER, params = Map(ADAPTIVE_NUMBER_CANDIDATE_KEY -> "5")),
          cardinality)
      }
      .addCase("Adaptive BloomFilter Write (15 Candidates)") { _ =>
        writeSkippingIndex(
          strategy(BLOOM_FILTER, params = Map(ADAPTIVE_NUMBER_CANDIDATE_KEY -> "15")),
          cardinality)
      }
      .run()
  }

  private def runReadBenchmark(cardinality: Int): Unit = {
    benchmark(s"Skipping Index Read $N Rows with Cardinality $cardinality", 1)
      .addCase("Partition Read") { _ =>
        readSkippingIndex(strategy(PARTITION), cardinality)
      }
      .addCase("MinMax Read") { _ =>
        readSkippingIndex(strategy(MIN_MAX), cardinality)
      }
      .addCase("ValueSet Read (Default Size 100)") { _ =>
        readSkippingIndex(strategy(VALUE_SET), cardinality)
      }
      .addCase("ValueSet Read (Unlimited Size)") { _ =>
        readSkippingIndex(
          strategy(VALUE_SET, Map(VALUE_SET_MAX_SIZE_KEY -> Integer.MAX_VALUE.toString)),
          cardinality)
      }
      .addCase("BloomFilter Read (1M NDV)") { _ =>
        readSkippingIndex(
          strategy(
            BLOOM_FILTER,
            Map(
              BLOOM_FILTER_ADAPTIVE_KEY -> "false",
              CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> N.toString)),
          cardinality)
      }
      .addCase("BloomFilter Read (Optimal NDV)") { _ =>
        readSkippingIndex(
          strategy(
            BLOOM_FILTER,
            Map(
              BLOOM_FILTER_ADAPTIVE_KEY -> "false",
              CLASSIC_BLOOM_FILTER_NUM_ITEMS_KEY -> cardinality.toString)),
          cardinality)
      }
      .addCase("Adaptive BloomFilter Read (Default 10 Candidates)") { _ =>
        readSkippingIndex(strategy(BLOOM_FILTER), cardinality)
      }
      .addCase("Adaptive BloomFilter Read (5 Candidates)") { _ =>
        readSkippingIndex(
          strategy(BLOOM_FILTER, params = Map(ADAPTIVE_NUMBER_CANDIDATE_KEY -> "5")),
          cardinality)
      }
      .addCase("Adaptive BloomFilter Read (15 Candidates)") { _ =>
        readSkippingIndex(
          strategy(BLOOM_FILTER, params = Map(ADAPTIVE_NUMBER_CANDIDATE_KEY -> "15")),
          cardinality)
      }
      .run()
  }

  private def benchmark(name: String, numRows: Int): BenchmarkBuilder = {
    new BenchmarkBuilder(new Benchmark(name, numRows, output = output))
  }

  /** Benchmark builder in fluent API style */
  private class BenchmarkBuilder(benchmark: Benchmark) {

    def addCase(name: String)(f: Int => Unit): BenchmarkBuilder = {
      benchmark.addCase(name)(f)
      this
    }

    def run(): Unit = {
      benchmark.run()
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
  }

  private def writeSkippingIndex(indexCol: FlintSparkSkippingStrategy, cardinality: Int): Unit = {
    /*
     * Generate N random numbers with the given cardinality and build single skipping index
     * data structure without group by.
     */
    val namedAggCols = getNamedAggColumn(indexCol)
    spark
      .range(N)
      .withColumn(testColName, (rand() * cardinality + 1).cast(testColType))
      .agg(namedAggCols.head, namedAggCols.tail: _*)
      .write
      .format(FLINT_DATASOURCE)
      .options(openSearchOptions)
      .mode(Overwrite)
      .save(getTestIndexName(indexCol, cardinality))
  }

  private def getTestIndexName(indexCol: FlintSparkSkippingStrategy, cardinality: Int): String = {
    // Generate unique name as skipping index name in OpenSearch
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
