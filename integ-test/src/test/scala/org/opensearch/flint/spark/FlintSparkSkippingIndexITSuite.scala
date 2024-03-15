/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.Base64

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.json4s.native.JsonMethods._
import org.opensearch.client.RequestOptions
import org.opensearch.flint.core.FlintVersion.current
import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN
import org.opensearch.flint.spark.skipping.FlintSparkSkippingFileIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilterMightContain.bloom_filter_might_contain
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.DeleteByQueryRequest
import org.scalatest.matchers.{Matcher, MatchResult}
import org.scalatest.matchers.must.Matchers._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.flint.config.FlintSparkConf._
import org.apache.spark.sql.functions.{col, isnull, lit, xxhash64}
import org.apache.spark.sql.internal.SQLConf

class FlintSparkSkippingIndexITSuite extends FlintSparkSuite {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.test"
  private val testIndex = getSkippingIndexName(testTable)
  private val testLatestId = Base64.getEncoder.encodeToString(testIndex.getBytes)

  override def beforeEach(): Unit = {
    super.beforeEach()
    createPartitionedMultiRowTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()

    // Delete all test indices
    deleteTestIndex(testIndex)
    sql(s"DROP TABLE $testTable")
  }

  test("create skipping index with metadata successfully") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .addValueSet("address")
      .addMinMax("age")
      .addBloomFilter("name")
      .create()

    val index = flint.describeIndex(testIndex)
    index shouldBe defined
    index.get.metadata().getContent should matchJson(s"""{
        |   "_meta": {
        |     "name": "flint_spark_catalog_default_test_skipping_index",
        |     "version": "${current()}",
        |     "kind": "skipping",
        |     "indexedColumns": [
        |     {
        |        "kind": "PARTITION",
        |        "parameters": {},
        |        "columnName": "year",
        |        "columnType": "int"
        |     },
        |     {
        |        "kind": "PARTITION",
        |        "parameters": {},
        |        "columnName": "month",
        |        "columnType": "int"
        |     },
        |     {
        |        "kind": "VALUE_SET",
        |        "parameters": { "max_size": "100" },
        |        "columnName": "address",
        |        "columnType": "string"
        |     },
        |     {
        |        "kind": "MIN_MAX",
        |        "parameters": {},
        |        "columnName": "age",
        |        "columnType": "int"
        |     },
        |     {
        |        "kind": "BLOOM_FILTER",
        |        "parameters": {
        |          "adaptive": "true",
        |          "num_candidates": "10",
        |          "fpp": "0.03"
        |        },
        |        "columnName": "name",
        |        "columnType": "string"
        |     }],
        |     "source": "spark_catalog.default.test",
        |     "options": {
        |       "auto_refresh": "false",
        |       "incremental_refresh": "false"
        |     },
        |     "latestId": "$testLatestId",
        |     "properties": {}
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
        |     "name" : {
        |       "type": "binary",
        |       "doc_values": true
        |     },
        |     "file_path": {
        |       "type": "keyword"
        |     }
        |   }
        | }
        |""".stripMargin)

    index.get.options shouldBe FlintSparkIndexOptions(
      Map("auto_refresh" -> "false", "incremental_refresh" -> "false"))
  }

  test("create skipping index with index options successfully") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addValueSet("address")
      .options(FlintSparkIndexOptions(Map(
        "auto_refresh" -> "true",
        "refresh_interval" -> "1 Minute",
        "checkpoint_location" -> "s3a://test/",
        "index_settings" -> "{\"number_of_shards\": 3,\"number_of_replicas\": 2}")))
      .create()

    val index = flint.describeIndex(testIndex)
    index shouldBe defined
    val optionJson = compact(render(parse(index.get.metadata().getContent) \ "_meta" \ "options"))
    optionJson should matchJson("""
        | {
        |   "auto_refresh": "true",
        |   "incremental_refresh": "false",
        |   "refresh_interval": "1 Minute",
        |   "checkpoint_location": "s3a://test/",
        |   "index_settings": "{\"number_of_shards\": 3,\"number_of_replicas\": 2}"
        | }
        |""".stripMargin)

    // Load index options from index mapping (verify OS index setting in SQL IT)
    index.get.options.autoRefresh() shouldBe true
    index.get.options.refreshInterval() shouldBe Some("1 Minute")
    index.get.options.checkpointLocation() shouldBe Some("s3a://test/")
    index.get.options.indexSettings() shouldBe
      Some("{\"number_of_shards\": 3,\"number_of_replicas\": 2}")
  }

  test("should not have ID column in index data") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year")
      .create()
    flint.refreshIndex(testIndex)

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

    val jobId = flint.refreshIndex(testIndex)
    jobId shouldBe empty

    val indexData = flint.queryIndex(testIndex).collect().toSet
    indexData should have size 2
  }

  test("incremental refresh skipping index successfully") {
    withTempDir { checkpointDir =>
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year", "month")
        .options(
          FlintSparkIndexOptions(
            Map(
              "incremental_refresh" -> "true",
              "checkpoint_location" -> checkpointDir.getAbsolutePath)))
        .create()

      flint.refreshIndex(testIndex) shouldBe empty
      flint.queryIndex(testIndex).collect().toSet should have size 2

      // Delete all index data intentionally and generate a new source file
      openSearchClient.deleteByQuery(
        new DeleteByQueryRequest(testIndex).setQuery(QueryBuilders.matchAllQuery()),
        RequestOptions.DEFAULT)
      sql(s"""
             | INSERT INTO $testTable
             | PARTITION (year=2023, month=4)
             | VALUES ('Hello', 35, 'Vancouver')
             | """.stripMargin)

      // Expect to only refresh the new file
      flint.refreshIndex(testIndex) shouldBe empty
      flint.queryIndex(testIndex).collect().toSet should have size 1
    }
  }

  test("should fail if incremental refresh without checkpoint location") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("incremental_refresh" -> "true")))
      .create()

    assertThrows[IllegalStateException] {
      flint.refreshIndex(testIndex)
    }
  }

  test("auto refresh skipping index successfully") {
    // Create Flint index and wait for complete
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()

    val jobId = flint.refreshIndex(testIndex)
    jobId shouldBe defined

    val job = spark.streams.get(jobId.get)
    failAfter(streamingTimeout) {
      job.processAllAvailable()
    }

    val indexData = flint.queryIndex(testIndex).collect().toSet
    indexData should have size 2
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
    flint.refreshIndex(testIndex)

    // Assert index data
    checkAnswer(
      flint.queryIndex(testIndex).select("year", "month"),
      Seq(Row(2023, 4), Row(2023, 5)))

    // Assert query rewrite
    val query = sql(s"""
                       | SELECT name
                       | FROM $testTable
                       | WHERE year = 2023 AND month = 4
                       |""".stripMargin)

    checkAnswer(query, Seq(Row("Hello"), Row("World")))
    query.queryExecution.executedPlan should
      useFlintSparkSkippingFileIndex(hasIndexFilter(col("year") === 2023 && col("month") === 4))
  }

  test("can build value set skipping index and rewrite applicable query") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addValueSet("address", Map("max_size" -> "2"))
      .create()
    flint.refreshIndex(testIndex)

    // Assert index data
    checkAnswer(
      flint.queryIndex(testIndex).select("address"),
      Seq(
        Row("""["Seattle","Portland"]"""),
        Row(null) // Value set exceeded limit size is expected to be null
      ))

    // Assert query rewrite that works with value set maybe null
    val query = sql(s"""
                       | SELECT age
                       | FROM $testTable
                       | WHERE address = 'Portland'
                       |""".stripMargin)

    query.queryExecution.executedPlan should
      useFlintSparkSkippingFileIndex(
        hasIndexFilter(isnull(col("address")) || col("address") === "Portland"))
    checkAnswer(query, Seq(Row(30), Row(50)))
  }

  test("can build min max skipping index and rewrite applicable query") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addMinMax("age")
      .create()
    flint.refreshIndex(testIndex)

    // Assert index data
    checkAnswer(
      flint.queryIndex(testIndex).select("MinMax_age_0", "MinMax_age_1"),
      Seq(Row(20, 30), Row(40, 60)))

    // Assert query rewrite
    val query = sql(s"""
                       | SELECT name
                       | FROM $testTable
                       | WHERE age = 30
                       |""".stripMargin)

    checkAnswer(query, Row("World"))
    query.queryExecution.executedPlan should
      useFlintSparkSkippingFileIndex(
        hasIndexFilter(col("MinMax_age_0") <= 30 && col("MinMax_age_1") >= 30))
  }

  test("can build bloom filter skipping index and rewrite applicable query") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addBloomFilter("age")
      .create()
    flint.refreshIndex(testIndex)

    // Assert index data
    flint.queryIndex(testIndex).collect() should have size 2

    // Assert query result and rewrite
    def assertQueryRewrite(): Unit = {
      val query = sql(s"SELECT name FROM $testTable WHERE age = 50")
      checkAnswer(query, Row("Java"))
      query.queryExecution.executedPlan should
        useFlintSparkSkippingFileIndex(
          hasIndexFilter(bloom_filter_might_contain("age", xxhash64(lit(50)))))
    }

    // Test expression with codegen enabled by default
    assertQueryRewrite()

    // Test expression evaluation with codegen disabled
    withSQLConf(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString) {
      assertQueryRewrite()
    }
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
      useFlintSparkSkippingFileIndex(hasIndexFilter(col("year") === 2023))
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
    flint.refreshIndex(testIndex)

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

      checkAnswer(query, Seq(Row("Seattle"), Row("Portland"), Row("Vancouver")))
    }
  }

  test("should return empty if describe index not exist") {
    flint.describeIndex("non-exist") shouldBe empty
  }

  test("create skipping index for all supported data types successfully") {
    // Prepare test table
    val testTable = "spark_catalog.default.data_type_table"
    val testIndex = getSkippingIndexName(testTable)
    val testLatestId = Base64.getEncoder.encodeToString(testIndex.getBytes)
    sql(s"""
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
    sql(s"""
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
    index.get.metadata().getContent should matchJson(s"""{
         |   "_meta": {
         |     "name": "flint_spark_catalog_default_data_type_table_skipping_index",
         |     "version": "${current()}",
         |     "kind": "skipping",
         |     "indexedColumns": [
         |     {
         |        "kind": "VALUE_SET",
         |        "parameters": { "max_size": "100" },
         |        "columnName": "boolean_col",
         |        "columnType": "boolean"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "parameters": { "max_size": "100" },
         |        "columnName": "string_col",
         |        "columnType": "string"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "parameters": { "max_size": "100" },
         |        "columnName": "varchar_col",
         |        "columnType": "varchar(20)"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "parameters": { "max_size": "100" },
         |        "columnName": "char_col",
         |        "columnType": "char(20)"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "parameters": { "max_size": "100" },
         |        "columnName": "long_col",
         |        "columnType": "bigint"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "parameters": { "max_size": "100" },
         |        "columnName": "int_col",
         |        "columnType": "int"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "parameters": { "max_size": "100" },
         |        "columnName": "short_col",
         |        "columnType": "smallint"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "parameters": { "max_size": "100" },
         |        "columnName": "byte_col",
         |        "columnType": "tinyint"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "parameters": { "max_size": "100" },
         |        "columnName": "double_col",
         |        "columnType": "double"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "parameters": { "max_size": "100" },
         |        "columnName": "float_col",
         |        "columnType": "float"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "parameters": { "max_size": "100" },
         |        "columnName": "timestamp_col",
         |        "columnType": "timestamp"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "parameters": { "max_size": "100" },
         |        "columnName": "date_col",
         |        "columnType": "date"
         |     },
         |     {
         |        "kind": "VALUE_SET",
         |        "parameters": { "max_size": "100" },
         |        "columnName": "struct_col",
         |        "columnType": "struct<subfield1:string,subfield2:int>"
         |     }],
         |     "source": "$testTable",
         |     "options": {
         |       "auto_refresh": "false",
         |       "incremental_refresh": "false"
         |     },
         |     "latestId": "$testLatestId",
         |     "properties": {}
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

    deleteTestIndex(testIndex)
  }

  test("can build skipping index for varchar and char and rewrite applicable query") {
    val testTable = "spark_catalog.default.varchar_char_table"
    val testIndex = getSkippingIndexName(testTable)
    sql(s"""
         | CREATE TABLE $testTable
         | (
         |   varchar_col VARCHAR(20),
         |   char_col CHAR(20)
         | )
         | USING PARQUET
         |""".stripMargin)
    sql(s"""
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
    flint.refreshIndex(testIndex)

    val query = sql(s"""
         | SELECT varchar_col, char_col
         | FROM $testTable
         | WHERE varchar_col = "sample varchar" AND char_col = "sample char"
         |""".stripMargin)

    // CharType column is padded to a fixed length with whitespace
    val paddedChar = "sample char".padTo(20, ' ')
    checkAnswer(query, Row("sample varchar", paddedChar))
    query.queryExecution.executedPlan should
      useFlintSparkSkippingFileIndex(
        hasIndexFilter((isnull(col("varchar_col")) || col("varchar_col") === "sample varchar") &&
          (isnull(col("char_col")) || col("char_col") === paddedChar)))

    deleteTestIndex(testIndex)
  }

  test("build skipping index for nested field and rewrite applicable query") {
    val testTable = "spark_catalog.default.nested_field_table"
    val testIndex = getSkippingIndexName(testTable)
    withTable(testTable) {
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

      flint
        .skippingIndex()
        .onTable(testTable)
        .addMinMax("struct_col.field2")
        .addValueSet("struct_col.field1.subfield")
        .create()
      flint.refreshIndex(testIndex)

      // FIXME: add assertion on index data once https://github.com/opensearch-project/opensearch-spark/issues/233 fixed
      // Query rewrite nested field
      val query1 =
        sql(s"SELECT int_col FROM $testTable WHERE struct_col.field2 = 456".stripMargin)
      checkAnswer(query1, Row(40))
      query1.queryExecution.executedPlan should
        useFlintSparkSkippingFileIndex(
          hasIndexFilter(
            col("MinMax_struct_col.field2_0") <= 456 && col("MinMax_struct_col.field2_1") >= 456))

      // Query rewrite deep nested field
      val query2 = sql(
        s"SELECT int_col FROM $testTable WHERE struct_col.field1.subfield = 'value3'".stripMargin)
      checkAnswer(query2, Row(50))
      query2.queryExecution.executedPlan should
        useFlintSparkSkippingFileIndex(
          hasIndexFilter(isnull(col("struct_col.field1.subfield")) ||
            col("struct_col.field1.subfield") === "value3"))

      deleteTestIndex(testIndex)
    }
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
        s"FlintSparkSkippingFileIndex does not have expected filter: ${fileIndex.indexFilter}",
        s"FlintSparkSkippingFileIndex has expected filter: ${fileIndex.indexFilter}")
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
