/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.mv

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.spark.FlintSparkIndexOptions
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.MV_INDEX_TYPE
import org.opensearch.flint.spark.mv.FlintSparkMaterializedViewSuite.{streamingRelation, StreamingDslLogicalPlan}
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, the}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.dsl.expressions.{count, intToLiteral, stringToLiteral, DslAttr, DslExpression, StringToAttributeConversionHelper}
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

  val testMvName = "spark_catalog.default.mv"
  val testQuery = "SELECT 1"

  test("get name") {
    val mv = FlintSparkMaterializedView(testMvName, testQuery, Map.empty)
    mv.name() shouldBe "flint_spark_catalog_default_mv"
  }

  test("should fail if get name with unqualified MV name") {
    the[IllegalArgumentException] thrownBy
      FlintSparkMaterializedView("mv", testQuery, Map.empty).name()

    the[IllegalArgumentException] thrownBy
      FlintSparkMaterializedView("default.mv", testQuery, Map.empty).name()
  }

  test("get metadata") {
    val mv = FlintSparkMaterializedView(testMvName, testQuery, Map("test_col" -> "integer"))

    val metadata = mv.metadata()
    metadata.name shouldBe mv.mvName
    metadata.kind shouldBe MV_INDEX_TYPE
    metadata.source shouldBe "SELECT 1"
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
      Map("test_col" -> "integer"),
      indexOptions)

    mv.metadata().options shouldBe Map(
      "auto_refresh" -> "true",
      "index_settings" -> indexSettings).asJava
    mv.metadata().indexSettings shouldBe Some(indexSettings)
  }

  test("build batch data frame") {
    val mv = FlintSparkMaterializedView(testMvName, testQuery, Map.empty)
    mv.build(spark, None).collect() shouldBe Array(Row(1))
  }

  test("should fail if build given other source data frame") {
    val mv = FlintSparkMaterializedView(testMvName, testQuery, Map.empty)
    the[IllegalArgumentException] thrownBy mv.build(spark, Some(mock[DataFrame]))
  }

  test("build stream should insert watermark operator and replace batch relation") {
    val testTable = "mv_build_test"
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (time TIMESTAMP, name STRING, age INT) USING CSV")

      val testQuery =
        s"""
          | SELECT
          |   window.start AS startTime,
          |   COUNT(*) AS count
          | FROM $testTable
          | GROUP BY TUMBLE(time, '1 Minute')
          |""".stripMargin

      val mv = FlintSparkMaterializedView(
        testMvName,
        testQuery,
        Map.empty,
        FlintSparkIndexOptions(Map("watermark_delay" -> "30 Seconds")))

      val actualPlan = mv.buildStream(spark).queryExecution.logical
      assert(
        actualPlan.sameSemantics(
          streamingRelation(testTable)
            .watermark($"time", "30 Seconds")
            .groupBy($"TUMBLE".function($"time", "1 Minute"))(
              $"window.start" as "startTime",
              count(1) as "count")))
    }
  }

  test("build stream with filtering aggregate query") {
    val testTable = "mv_build_test"
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (time TIMESTAMP, name STRING, age INT) USING CSV")

      val testQuery =
        s"""
           | SELECT
           |   window.start AS startTime,
           |   COUNT(*) AS count
           | FROM $testTable
           | WHERE age > 30
           | GROUP BY TUMBLE(time, '1 Minute')
           |""".stripMargin

      val mv = FlintSparkMaterializedView(
        testMvName,
        testQuery,
        Map.empty,
        FlintSparkIndexOptions(Map("watermark_delay" -> "30 Seconds")))

      val actualPlan = mv.buildStream(spark).queryExecution.logical
      assert(
        actualPlan.sameSemantics(
          streamingRelation(testTable)
            .where($"age" > 30)
            .watermark($"time", "30 Seconds")
            .groupBy($"TUMBLE".function($"time", "1 Minute"))(
              $"window.start" as "startTime",
              count(1) as "count")))
    }
  }

  test("build stream with non-aggregate query") {
    val testTable = "mv_build_test"
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (time TIMESTAMP, name STRING, age INT) USING CSV")

      val mv = FlintSparkMaterializedView(
        testMvName,
        s"SELECT name, age FROM $testTable WHERE age > 30",
        Map.empty)
      val actualPlan = mv.buildStream(spark).queryExecution.logical

      assert(
        actualPlan.sameSemantics(
          streamingRelation(testTable)
            .where($"age" > 30)
            .select($"name", $"age")))
    }
  }

  test("build stream should fail if there is aggregation but no windowing function") {
    val testTable = "mv_build_test"
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (time TIMESTAMP, name STRING, age INT) USING CSV")

      val mv = FlintSparkMaterializedView(
        testMvName,
        s"SELECT name, COUNT(*) AS count FROM $testTable GROUP BY name",
        Map.empty)

      the[IllegalStateException] thrownBy
        mv.buildStream(spark)
    }
  }
}

/**
 * Helper method that extends LogicalPlan with more methods by Scala implicit class.
 */
object FlintSparkMaterializedViewSuite {

  def streamingRelation(tableName: String): UnresolvedRelation = {
    UnresolvedRelation(
      TableIdentifier(tableName),
      CaseInsensitiveStringMap.empty(),
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
