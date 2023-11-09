/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.{getSkippingIndexName, SKIPPING_INDEX_TYPE}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.SkippingKind
import org.scalatest.matchers.{Matcher, MatchResult}
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression, ExprId, Literal, Or}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class ApplyFlintSparkSkippingIndexSuite extends SparkFunSuite with Matchers {

  /** Test table and index */
  private val testTable = "spark_catalog.default.apply_skipping_index_test"
  private val testIndex = getSkippingIndexName(testTable)
  private val testSchema = StructType(
    Seq(
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("address", StringType, nullable = false)))

  /** Resolved column reference used in filtering condition */
  private val nameCol =
    AttributeReference("name", StringType, nullable = false)(exprId = ExprId(1))
  private val ageCol =
    AttributeReference("age", IntegerType, nullable = false)(exprId = ExprId(2))
  private val addressCol =
    AttributeReference("address", StringType, nullable = false)(exprId = ExprId(3))

  test("should not rewrite query if no skipping index") {
    assertFlintQueryRewriter()
      .withSourceTable(testTable, testSchema)
      .withFilter(EqualTo(nameCol, Literal("hello")))
      .withNoSkippingIndex()
      .shouldNotRewrite()
  }

  test("should not rewrite query if filter condition is disjunction") {
    assertFlintQueryRewriter()
      .withSourceTable(testTable, testSchema)
      .withFilter(Or(EqualTo(nameCol, Literal("hello")), EqualTo(ageCol, Literal(30))))
      .withSkippingIndex(testIndex, "name", "age")
      .shouldNotRewrite()
  }

  test("should not rewrite query if filter condition contains disjunction") {
    assertFlintQueryRewriter()
      .withSourceTable(testTable, testSchema)
      .withFilter(
        And(
          Or(EqualTo(nameCol, Literal("hello")), EqualTo(ageCol, Literal(30))),
          EqualTo(ageCol, Literal(30))))
      .withSkippingIndex(testIndex, "name", "age")
      .shouldNotRewrite()
  }

  test("should rewrite query with skipping index") {
    assertFlintQueryRewriter()
      .withSourceTable(testTable, testSchema)
      .withFilter(EqualTo(nameCol, Literal("hello")))
      .withSkippingIndex(testIndex, "name")
      .shouldPushDownAfterRewrite(col("name") === "hello")
  }

  test("should only push down filter condition with indexed column") {
    assertFlintQueryRewriter()
      .withSourceTable(testTable, testSchema)
      .withFilter(And(EqualTo(nameCol, Literal("hello")), EqualTo(ageCol, Literal(30))))
      .withSkippingIndex(testIndex, "name")
      .shouldPushDownAfterRewrite(col("name") === "hello")
  }

  test("should push down all filter conditions with indexed column") {
    assertFlintQueryRewriter()
      .withSourceTable(testTable, testSchema)
      .withFilter(And(EqualTo(nameCol, Literal("hello")), EqualTo(ageCol, Literal(30))))
      .withSkippingIndex(testIndex, "name", "age")
      .shouldPushDownAfterRewrite(col("name") === "hello" && col("age") === 30)

    assertFlintQueryRewriter()
      .withSourceTable(testTable, testSchema)
      .withFilter(
        And(
          EqualTo(nameCol, Literal("hello")),
          And(EqualTo(ageCol, Literal(30)), EqualTo(addressCol, Literal("Seattle")))))
      .withSkippingIndex(testIndex, "name", "age", "address")
      .shouldPushDownAfterRewrite(
        col("name") === "hello" && col("age") === 30 && col("address") === "Seattle")
  }

  private def assertFlintQueryRewriter(): AssertionHelper = {
    new AssertionHelper
  }

  private class AssertionHelper {
    private val flint = {
      val mockFlint = mock[FlintSpark](RETURNS_DEEP_STUBS)
      when(mockFlint.spark.sessionState.catalogManager.currentCatalog.name())
        .thenReturn("spark_catalog")
      mockFlint
    }
    private val rule = new ApplyFlintSparkSkippingIndex(flint)
    private var relation: LogicalRelation = _
    private var plan: LogicalPlan = _

    def withSourceTable(fullname: String, schema: StructType): AssertionHelper = {
      val table = CatalogTable(
        identifier = TableIdentifier(fullname.split('.')(1), Some(fullname.split('.')(0))),
        tableType = CatalogTableType.EXTERNAL,
        storage = CatalogStorageFormat.empty,
        schema = null)
      relation = LogicalRelation(mockBaseRelation(schema), table)
      this
    }

    def withFilter(condition: Expression): AssertionHelper = {
      val filter = Filter(condition, relation)
      val project = Project(Seq(), filter)
      plan = SubqueryAlias("alb_logs", project)
      this
    }

    def withSkippingIndex(indexName: String, indexCols: String*): AssertionHelper = {
      val skippingIndex = mock[FlintSparkSkippingIndex]
      when(skippingIndex.kind).thenReturn(SKIPPING_INDEX_TYPE)
      when(skippingIndex.name()).thenReturn(indexName)
      when(skippingIndex.indexedColumns).thenReturn(indexCols.map(FakeSkippingStrategy))
      when(skippingIndex.filterCondition).thenReturn(None)

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

  private def mockBaseRelation(schema: StructType): BaseRelation = {
    val fileIndex = mock[FileIndex]
    val baseRelation: HadoopFsRelation = mock[HadoopFsRelation]
    when(baseRelation.location).thenReturn(fileIndex)
    when(baseRelation.schema).thenReturn(schema)

    // Mock baseRelation.copy(location = FlintFileIndex)
    doAnswer((invocation: InvocationOnMock) => {
      val location = invocation.getArgument[FileIndex](0)
      val relationCopy: HadoopFsRelation = mock[HadoopFsRelation]
      when(relationCopy.location).thenReturn(location)
      relationCopy
    }).when(baseRelation).copy(any(), any(), any(), any(), any(), any())(any())

    baseRelation
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
