/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{mockStatic, when, RETURNS_DEEP_STUBS}
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder, FlintOptions}
import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.getFlintIndexName
import org.scalatest.matchers.{Matcher, MatchResult}
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parseExpression
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

class ApplyFlintSparkCoveringIndexSuite extends FlintSuite with Matchers {

  private val testTable = "spark_catalog.default.apply_covering_index_test"

  private val clientBuilder = mockStatic(classOf[FlintClientBuilder])
  private val client = mock[FlintClient](RETURNS_DEEP_STUBS)

  /** Mock the FlintSpark dependency */
  private val flint = mock[FlintSpark]

  /** Instantiate the rule once for all tests */
  private val rule = new ApplyFlintSparkCoveringIndex(flint)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    clientBuilder
      .when(() => FlintClientBuilder.build(any(classOf[FlintOptions])))
      .thenReturn(client)
    when(flint.spark).thenReturn(spark)
  }

  override protected def afterAll(): Unit = {
    clientBuilder.close()
    super.afterAll()
  }

  test("should not apply if no index present") {
    assertFlintQueryRewriter
      .withTable(StructType.fromDDL("name STRING, age INT"))
      .assertIndexNotUsed()
  }

  test("should not apply if some columns in project are not covered") {
    assertFlintQueryRewriter
      .withTable(StructType.fromDDL("name STRING, age INT, city STRING"))
      .withProject("name", "age", "city") // city is not covered
      .withIndex(
        new FlintSparkCoveringIndex(
          indexName = "partial",
          tableName = testTable,
          indexedColumns = Map("name" -> "string", "age" -> "int")))
      .assertIndexNotUsed()
  }

  test("should not apply if some columns in filter are not covered") {
    assertFlintQueryRewriter
      .withTable(StructType.fromDDL("name STRING, age INT, city STRING"))
      .withFilter("city = 'Seattle'")
      .withProject("name", "age")
      .withIndex(
        new FlintSparkCoveringIndex(
          indexName = "all",
          tableName = testTable,
          indexedColumns = Map("name" -> "string", "age" -> "int")))
      .assertIndexNotUsed()
  }

  test("should apply when all columns are covered") {
    assertFlintQueryRewriter
      .withTable(StructType.fromDDL("name STRING, age INT"))
      .withProject("name", "age")
      .withIndex(
        new FlintSparkCoveringIndex(
          indexName = "all",
          tableName = testTable,
          indexedColumns = Map("name" -> "string", "age" -> "int")))
      .assertIndexUsed(getFlintIndexName("all", testTable))
  }

  private def assertFlintQueryRewriter: AssertionHelper = new AssertionHelper

  class AssertionHelper {
    private var schema: StructType = _
    private var plan: LogicalPlan = _
    private var indexes: Seq[FlintSparkCoveringIndex] = Seq()

    def withTable(schema: StructType): AssertionHelper = {
      val baseRelation = mock[BaseRelation]
      val table = mock[CatalogTable]
      when(baseRelation.schema).thenReturn(schema)
      when(table.qualifiedName).thenReturn(testTable)

      this.schema = schema
      this.plan = LogicalRelation(baseRelation, table)
      this
    }

    def withProject(colNames: String*): AssertionHelper = {
      val output = colNames.map(name => AttributeReference(name, schema(name).dataType)())
      this.plan = Project(output, plan)
      this
    }

    def withFilter(predicate: String): AssertionHelper = {
      this.plan = Filter(parseExpression(predicate), plan)
      this
    }

    def withIndex(index: FlintSparkCoveringIndex): AssertionHelper = {
      this.indexes = indexes :+ index
      this
    }

    def assertIndexUsed(expectedIndexName: String): AssertionHelper = {
      rewritePlan should scanIndexOnly(expectedIndexName)
      this
    }

    def assertIndexNotUsed(): AssertionHelper = {
      rewritePlan should scanSourceTable
      this
    }

    private def rewritePlan: LogicalPlan = {
      when(flint.describeIndexes(anyString())).thenReturn(indexes)
      indexes.foreach { index =>
        when(client.getIndexMetadata(index.name())).thenReturn(index.metadata())
      }
      rule.apply(plan)
    }

    private def scanSourceTable: Matcher[LogicalPlan] = {
      Matcher { (plan: LogicalPlan) =>
        val result = plan.exists {
          case LogicalRelation(_, _, Some(table), _) =>
            table.qualifiedName == testTable
          case _ => false
        }

        MatchResult(
          result,
          s"Plan does not scan table $testTable",
          s"Plan scans table $testTable as expected")
      }
    }

    private def scanIndexOnly(expectedIndexName: String): Matcher[LogicalPlan] = {
      Matcher { (plan: LogicalPlan) =>
        val result = plan.exists {
          case relation: DataSourceV2Relation =>
            relation.table.name() == expectedIndexName
          case _ => false
        }

        MatchResult(
          result,
          s"Plan does not scan index $expectedIndexName only",
          s"Plan scan index $expectedIndexName only as expected")
      }
    }
  }
}
