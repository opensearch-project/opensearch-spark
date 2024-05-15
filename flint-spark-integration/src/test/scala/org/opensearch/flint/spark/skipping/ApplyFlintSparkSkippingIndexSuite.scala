/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder, FlintOptions}
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState.{DELETED, IndexState, REFRESHING}
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndexOptions}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.SkippingKind
import org.scalatest.matchers.{Matcher, MatchResult}
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.col

class ApplyFlintSparkSkippingIndexSuite extends FlintSuite with Matchers {

  /** Test table and index */
  private val testTable = "spark_catalog.default.apply_skipping_index_test"

  // Mock FlintClient to avoid looking for real OpenSearch cluster
  private val clientBuilder = mockStatic(classOf[FlintClientBuilder])
  private val client = mock[FlintClient](RETURNS_DEEP_STUBS)

  /** Mock FlintSpark which is required by the rule */
  private val flint = mock[FlintSpark]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE TABLE $testTable (name STRING, age INT, address STRING) USING JSON")

    // Mock static create method in FlintClientBuilder used by Flint data source
    clientBuilder
      .when(() => FlintClientBuilder.build(any(classOf[FlintOptions])))
      .thenReturn(client)
    when(flint.spark).thenReturn(spark)
  }

  override protected def afterAll(): Unit = {
    sql(s"DROP TABLE $testTable")
    clientBuilder.close()
    super.afterAll()
  }

  test("should not rewrite query if no skipping index") {
    assertFlintQueryRewriter()
      .withQuery(s"SELECT * FROM $testTable WHERE name = 'hello'")
      .withNoSkippingIndex()
      .shouldNotRewrite()
  }

  test("should not rewrite query if filter condition is disjunction") {
    assertFlintQueryRewriter()
      .withQuery(s"SELECT * FROM $testTable WHERE name = 'hello' or age = 30")
      .withSkippingIndex(REFRESHING, "name", "age")
      .shouldNotRewrite()
  }

  test("should not rewrite query if filter condition contains disjunction") {
    assertFlintQueryRewriter()
      .withQuery(
        s"SELECT * FROM $testTable WHERE (name = 'hello' or age = 30) and address = 'Seattle'")
      .withSkippingIndex(REFRESHING, "name", "age")
      .shouldNotRewrite()
  }

  test("should rewrite query with skipping index") {
    assertFlintQueryRewriter()
      .withQuery(s"SELECT * FROM $testTable WHERE name = 'hello'")
      .withSkippingIndex(REFRESHING, "name")
      .shouldPushDownAfterRewrite(col("name") === "hello")
  }

  test("should not rewrite query with deleted skipping index") {
    assertFlintQueryRewriter()
      .withQuery(s"SELECT * FROM $testTable WHERE name = 'hello'")
      .withSkippingIndex(DELETED, "name")
      .shouldNotRewrite()
  }

  test("should only push down filter condition with indexed column") {
    assertFlintQueryRewriter()
      .withQuery(s"SELECT * FROM $testTable WHERE name = 'hello' and age = 30")
      .withSkippingIndex(REFRESHING, "name")
      .shouldPushDownAfterRewrite(col("name") === "hello")
  }

  test("should push down all filter conditions with indexed column") {
    assertFlintQueryRewriter()
      .withQuery(s"SELECT * FROM $testTable WHERE name = 'hello' and age = 30")
      .withSkippingIndex(REFRESHING, "name", "age")
      .shouldPushDownAfterRewrite(col("name") === "hello" && col("age") === 30)

    assertFlintQueryRewriter()
      .withQuery(
        s"SELECT * FROM $testTable WHERE name = 'hello' and (age = 30 and address = 'Seattle')")
      .withSkippingIndex(REFRESHING, "name", "age", "address")
      .shouldPushDownAfterRewrite(
        col("name") === "hello" && col("age") === 30 && col("address") === "Seattle")
  }

  private def assertFlintQueryRewriter(): AssertionHelper = {
    new AssertionHelper
  }

  private class AssertionHelper {
    private val rule = new ApplyFlintSparkSkippingIndex(flint)
    private var plan: LogicalPlan = _

    def withQuery(query: String): AssertionHelper = {
      this.plan = sql(query).queryExecution.optimizedPlan
      this
    }

    def withSkippingIndex(indexState: IndexState, indexCols: String*): AssertionHelper = {
      val skippingIndex = new FlintSparkSkippingIndex(
        tableName = testTable,
        indexedColumns = indexCols.map(FakeSkippingStrategy),
        options = FlintSparkIndexOptions.empty,
        latestLogEntry = Some(
          new FlintMetadataLogEntry("id", 0, 0, 0, indexState, "spark_catalog", "")))

      when(flint.describeIndex(any())).thenReturn(Some(skippingIndex))
      this
    }

    def withNoSkippingIndex(): AssertionHelper = {
      when(flint.describeIndex(any())).thenReturn(None)
      this
    }

    def shouldPushDownAfterRewrite(expected: Column): Unit = {
      rule.apply(plan) should pushDownFilterToIndexScan(expected)
    }

    def shouldNotRewrite(): Unit = {
      rule.apply(plan) shouldBe plan
    }
  }

  private def pushDownFilterToIndexScan(expect: Column): Matcher[LogicalPlan] = {
    Matcher { (plan: LogicalPlan) =>
      val useFlintSparkSkippingFileIndex = plan.exists {
        case LogicalRelation(
              HadoopFsRelation(fileIndex: FlintSparkSkippingFileIndex, _, _, _, _, _),
              _,
              _,
              _) if fileIndex.indexFilter.semanticEquals(expect.expr) =>
          true
        case _ => false
      }

      MatchResult(
        useFlintSparkSkippingFileIndex,
        "Plan does not use FlintSparkSkippingFileIndex with expected filter",
        "Plan uses FlintSparkSkippingFileIndex with expected filter")
    }
  }

  /** Fake strategy that simply push down given predicate on the column. */
  case class FakeSkippingStrategy(override val columnName: String)
      extends FlintSparkSkippingStrategy {

    override val kind: SkippingKind = null

    override val columnType: String = null

    override def outputSchema(): Map[String, String] = Map.empty

    override def getAggregators: Seq[AggregateFunction] = Seq.empty

    override def rewritePredicate(predicate: Expression): Option[Expression] =
      predicate match {
        case EqualTo(AttributeReference(`columnName`, _, _, _), value: Literal) =>
          Some((col(columnName) === value).expr)
        case _ => None
      }
  }
}
