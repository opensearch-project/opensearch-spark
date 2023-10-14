/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.mv

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.spark.FlintSparkIndexOptions
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.MV_INDEX_TYPE
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, the}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, DslExpression}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, EventTimeWatermark}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

class FlintSparkMaterializedViewSuite extends FlintSuite {

  val testMvName = "spark_catalog.default.mv"
  val testQuery = "SELECT 1"

  test("get name") {
    val mv = FlintSparkMaterializedView(testMvName, testQuery, Map.empty)
    mv.name() shouldBe "flint_spark_catalog_default_mv"
  }

  test("should fail if not full MV name") {
    val mv = FlintSparkMaterializedView("mv", "SELECT 1", Map.empty)
    assertThrows[IllegalArgumentException] {
      mv.name()
    }
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

      val mv = FlintSparkMaterializedView(testMvName, testQuery, Map.empty)
      val actualPlan = mv.buildStream(spark).queryExecution.logical

      val timeColumn = UnresolvedAttribute("time")
      val expectPlan =
        Aggregate(
          Seq(
            UnresolvedFunction(
              "TUMBLE",
              Seq(timeColumn, Literal("1 Minute")),
              isDistinct = false)),
          Seq(
            UnresolvedAttribute("window.start") as "startTime",
            UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false) as "count"),
          EventTimeWatermark(
            timeColumn,
            IntervalUtils.stringToInterval(UTF8String.fromString("0 Minute")),
            UnresolvedRelation(
              TableIdentifier(testTable),
              CaseInsensitiveStringMap.empty(),
              isStreaming = true)))

      actualPlan.sameSemantics(expectPlan)
    }
  }
}
