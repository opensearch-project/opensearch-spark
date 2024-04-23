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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

class ApplyFlintSparkCoveringIndexSuite extends FlintSuite with Matchers {

  /** Test table name */
  private val testTable = "spark_catalog.default.apply_covering_index_test"

  // Mock FlintClient to avoid looking for real OpenSearch cluster
  private val clientBuilder = mockStatic(classOf[FlintClientBuilder])
  private val client = mock[FlintClient](RETURNS_DEEP_STUBS)

  /** Mock FlintSpark which is required by the rule */
  private val flint = mock[FlintSpark]

  /** Instantiate the rule once for all tests */
  private val rule = new ApplyFlintSparkCoveringIndex(flint)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE TABLE $testTable (name STRING, age INT) USING JSON")

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

  test("should not apply if no index present") {
    assertFlintQueryRewriter
      .withQuery(s"SELECT name, age FROM $testTable")
      .assertIndexNotUsed()
  }

  // Covering index doesn't column age
  Seq(
    s"SELECT name, age FROM $testTable",
    s"SELECT name FROM $testTable WHERE age = 30",
    s"SELECT COUNT(*) FROM $testTable GROUP BY age").foreach { query =>
    test(s"should not apply if column is not covered in $query") {
      assertFlintQueryRewriter
        .withQuery(query)
        .withIndex(
          new FlintSparkCoveringIndex(
            indexName = "partial",
            tableName = testTable,
            indexedColumns = Map("name" -> "string")))
        .assertIndexNotUsed()
    }
  }

  // Covering index covers all columns
  Seq(
    s"SELECT * FROM $testTable",
    s"SELECT name, age FROM $testTable",
    s"SELECT name FROM $testTable WHERE age = 30",
    s"SELECT COUNT(*) FROM $testTable GROUP BY age",
    s"SELECT name, COUNT(*) FROM $testTable WHERE age > 30 GROUP BY name").foreach { query =>
    test(s"should apply when all columns are covered in $query") {
      assertFlintQueryRewriter
        .withQuery(query)
        .withIndex(
          new FlintSparkCoveringIndex(
            indexName = "all",
            tableName = testTable,
            indexedColumns = Map("name" -> "string", "age" -> "int")))
        .assertIndexUsed(getFlintIndexName("all", testTable))
    }
  }

  test("should apply if all columns are covered by one of the covering indexes") {
    assertFlintQueryRewriter
      .withQuery(s"SELECT name FROM $testTable")
      .withIndex(
        new FlintSparkCoveringIndex(
          indexName = "age",
          tableName = testTable,
          indexedColumns = Map("age" -> "int")))
      .withIndex(
        new FlintSparkCoveringIndex(
          indexName = "name",
          tableName = testTable,
          indexedColumns = Map("name" -> "string")))
      .assertIndexUsed(getFlintIndexName("name", testTable))
  }

  private def assertFlintQueryRewriter: AssertionHelper = new AssertionHelper

  class AssertionHelper {
    private var plan: LogicalPlan = _
    private var indexes: Seq[FlintSparkCoveringIndex] = Seq()

    def withQuery(query: String): AssertionHelper = {
      this.plan = sql(query).queryExecution.analyzed
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
            // Table name in logical relation doesn't have catalog name
            table.qualifiedName == testTable.split('.').drop(1).mkString(".")
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
