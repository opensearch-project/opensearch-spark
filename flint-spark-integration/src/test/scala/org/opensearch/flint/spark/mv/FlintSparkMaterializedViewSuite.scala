/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.mv

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

import org.opensearch.flint.spark.FlintSparkIndexOptions
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.MV_INDEX_TYPE
import org.opensearch.flint.spark.mv.FlintSparkMaterializedViewSuite.{streamingRelation, StreamingDslLogicalPlan}
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.dsl.expressions.{intToLiteral, stringToLiteral, DslAttr, DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, LogicalPlan}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

/**
 * This UT include test cases for building API which make use of real SparkSession. This is
 * because SparkSession.sessionState is private val and hard to mock but it's required in
 * logicalPlanToDataFrame() -> DataRows.of().
 */
class FlintSparkMaterializedViewSuite extends FlintSuite {

  /** Test table, MV name and query */
  val testTable = "spark_catalog.default.mv_build_test"
  val testMvName = "spark_catalog.default.mv"
  val testQuery = "SELECT 1"

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE TABLE $testTable (time TIMESTAMP, name STRING, age INT) USING CSV")
  }

  override def afterAll(): Unit = {
    sql(s"DROP TABLE $testTable")
    super.afterAll()
  }

  test("get mv name") {
    val mv = FlintSparkMaterializedView(testMvName, testQuery, Array.empty, Map.empty)
    mv.name() shouldBe "flint_spark_catalog_default_mv"
  }

  test("get mv name with dots") {
    val testMvNameDots = "spark_catalog.default.mv.2023.10"
    val mv = FlintSparkMaterializedView(testMvNameDots, testQuery, Array.empty, Map.empty)
    mv.name() shouldBe "flint_spark_catalog_default_mv.2023.10"
  }

  test("should fail if get name with unqualified MV name") {
    the[IllegalArgumentException] thrownBy
      FlintSparkMaterializedView("mv", testQuery, Array.empty, Map.empty).name()

    the[IllegalArgumentException] thrownBy
      FlintSparkMaterializedView("default.mv", testQuery, Array.empty, Map.empty).name()
  }

  test("get metadata") {
    val mv =
      FlintSparkMaterializedView(testMvName, testQuery, Array.empty, Map("test_col" -> "integer"))

    val metadata = mv.metadata()
    metadata.name shouldBe mv.mvName
    metadata.kind shouldBe MV_INDEX_TYPE
    metadata.source shouldBe "SELECT 1"
    metadata.properties should contain key "sourceTables"
    metadata.properties
      .get("sourceTables")
      .asInstanceOf[java.util.ArrayList[String]] should have size 0
    metadata.indexedColumns shouldBe Array(
      Map("columnName" -> "test_col", "columnType" -> "integer").asJava)
    metadata.schema shouldBe Map("test_col" -> Map("type" -> "integer").asJava).asJava
  }

  test("get metadata with index options") {
    val indexSettings = """{"number_of_shards": 2}"""
    val indexOptions =
      FlintSparkIndexOptions(Map("auto_refresh" -> "true", "index_settings" -> indexSettings))
    val mv = FlintSparkMaterializedView(
      testMvName,
      testQuery,
      Array.empty,
      Map("test_col" -> "integer"),
      indexOptions)

    mv.metadata().options.asScala should contain allOf ("auto_refresh" -> "true",
    "index_settings" -> indexSettings)
    mv.metadata().indexSettings shouldBe Some(indexSettings)
  }

  test("build batch data frame") {
    val mv = FlintSparkMaterializedView(testMvName, testQuery, Array.empty, Map.empty)
    mv.build(spark, None).collect() shouldBe Array(Row(1))
  }

  test("should fail if build given other source data frame") {
    val mv = FlintSparkMaterializedView(testMvName, testQuery, Array.empty, Map.empty)
    the[IllegalArgumentException] thrownBy mv.build(spark, Some(mock[DataFrame]))
  }

  test("build stream should insert watermark operator and replace batch relation") {
    val testQuery =
      s"""
          | SELECT
          |   window.start AS startTime,
          |   COUNT(*) AS count
          | FROM $testTable
          | GROUP BY TUMBLE(time, '1 Minute')
          |""".stripMargin
    val options = Map("watermark_delay" -> "30 Seconds")

    withAggregateMaterializedView(testQuery, Array(testTable), options) { actualPlan =>
      comparePlans(
        actualPlan,
        streamingRelation(testTable)
          .watermark($"time", "30 Seconds")
          .groupBy($"TUMBLE".function($"time", "1 Minute"))(
            $"window.start" as "startTime",
            $"COUNT".function(1) as "count"),
        checkAnalysis = false
      ) // don't analyze due to full test table name
    }
  }

  test("build stream with filtering aggregate query") {
    val testQuery =
      s"""
           | SELECT
           |   window.start AS startTime,
           |   COUNT(*) AS count
           | FROM $testTable
           | WHERE age > 30
           | GROUP BY TUMBLE(time, '1 Minute')
           |""".stripMargin
    val options = Map("watermark_delay" -> "30 Seconds")

    withAggregateMaterializedView(testQuery, Array(testTable), options) { actualPlan =>
      comparePlans(
        actualPlan,
        streamingRelation(testTable)
          .where($"age" > 30)
          .watermark($"time", "30 Seconds")
          .groupBy($"TUMBLE".function($"time", "1 Minute"))(
            $"window.start" as "startTime",
            $"COUNT".function(1) as "count"),
        checkAnalysis = false)
    }
  }

  test("build stream with non-aggregate query") {
    val testQuery = s"SELECT name, age FROM $testTable WHERE age > 30"

    withAggregateMaterializedView(testQuery, Array(testTable), Map.empty) { actualPlan =>
      comparePlans(
        actualPlan,
        streamingRelation(testTable)
          .where($"age" > 30)
          .select($"name", $"age"),
        checkAnalysis = false)
    }
  }

  test("build stream with extra source options") {
    val testQuery = s"SELECT name, age FROM $testTable"
    val options = Map("extra_options" -> s"""{"$testTable": {"maxFilesPerTrigger": "1"}}""")

    withAggregateMaterializedView(testQuery, Array(testTable), options) { actualPlan =>
      comparePlans(
        actualPlan,
        streamingRelation(testTable, Map("maxFilesPerTrigger" -> "1"))
          .select($"name", $"age"),
        checkAnalysis = false)
    }
  }

  test("build stream should fail if there is aggregation but no windowing function") {
    val mv = FlintSparkMaterializedView(
      testMvName,
      s"SELECT name, COUNT(*) AS count FROM $testTable GROUP BY name",
      Array(testTable),
      Map.empty)

    the[IllegalStateException] thrownBy
      mv.buildStream(spark)
  }

  test("build batch with ID expression option") {
    val testMvQuery = s"SELECT time, name FROM $testTable"
    val mv = FlintSparkMaterializedView(
      testMvName,
      testMvQuery,
      Array.empty,
      Map.empty,
      FlintSparkIndexOptions(Map("id_expression" -> "time")))

    val batchDf = mv.build(spark, None)
    batchDf.idColumn() shouldBe Some(UnresolvedAttribute(Seq("time")))
  }

  test("build batch should not have ID column if not provided") {
    val testMvQuery = s"SELECT time, name FROM $testTable"
    val mv = FlintSparkMaterializedView(testMvName, testMvQuery, Array.empty, Map.empty)

    val batchDf = mv.build(spark, None)
    batchDf.idColumn() shouldBe None
  }

  test("build stream with ID expression option") {
    val mv = FlintSparkMaterializedView(
      testMvName,
      s"SELECT time, name FROM $testTable",
      Array.empty,
      Map.empty,
      FlintSparkIndexOptions(Map("auto_refresh" -> "true", "id_expression" -> "time")))

    val streamDf = mv.buildStream(spark)
    streamDf.idColumn() shouldBe Some(UnresolvedAttribute(Seq("time")))
  }

  test("build stream should not have ID column if not provided") {
    val mv = FlintSparkMaterializedView(
      testMvName,
      s"SELECT time, name FROM $testTable",
      Array.empty,
      Map.empty,
      FlintSparkIndexOptions(Map("auto_refresh" -> "true")))

    val streamDf = mv.buildStream(spark)
    streamDf.idColumn() shouldBe None
  }

  private def withAggregateMaterializedView(
      query: String,
      sourceTables: Array[String],
      options: Map[String, String])(codeBlock: LogicalPlan => Unit): Unit = {

    val mv =
      FlintSparkMaterializedView(
        testMvName,
        query,
        sourceTables,
        Map.empty,
        FlintSparkIndexOptions(options))

    val actualPlan = mv.buildStream(spark).queryExecution.logical
    codeBlock(actualPlan)
  }
}

/**
 * Helper method that extends LogicalPlan with more methods by Scala implicit class.
 */
object FlintSparkMaterializedViewSuite {

  def streamingRelation(
      tableName: String,
      extraOptions: Map[String, String] = Map.empty): UnresolvedRelation = {
    new UnresolvedRelation(
      tableName.split('.'),
      new CaseInsensitiveStringMap(extraOptions.asJava),
      isStreaming = true)
  }

  implicit class StreamingDslLogicalPlan(val logicalPlan: LogicalPlan) {

    def watermark(colName: Attribute, interval: String): DslLogicalPlan = {
      EventTimeWatermark(
        colName,
        IntervalUtils.stringToInterval(UTF8String.fromString(interval)),
        logicalPlan)
    }
  }
}
