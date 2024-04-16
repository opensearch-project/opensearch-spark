/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{mockStatic, when, RETURNS_DEEP_STUBS}
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder, FlintOptions}
import org.opensearch.flint.spark.FlintSpark
import org.scalatest.matchers.{Matcher, MatchResult}
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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

  test("Covering index should be applied when all columns are covered") {
    val schema = StructType(Seq(StructField("name", StringType), StructField("age", IntegerType)))
    val baseRelation = mock[BaseRelation]
    when(baseRelation.schema).thenReturn(schema)

    val table = mock[CatalogTable]
    // when(table.identifier).thenReturn(TableIdentifier("test_table", Some("default")))
    when(table.qualifiedName).thenReturn("default.test_table")

    val logicalRelation = LogicalRelation(baseRelation, table)
    when(flint.describeIndexes(anyString())).thenReturn(
      Seq(
        new FlintSparkCoveringIndex(
          indexName = "test_index",
          tableName = "spark_catalog.default.test_table",
          indexedColumns = Map("name" -> "string", "age" -> "int"),
          filterCondition = None)))

    when(client.getIndexMetadata(anyString()).getContent).thenReturn(s"""
         | {
         |   "properties": {
         |     "name": {
         |       "type": "keyword"
         |     },
         |     "age": {
         |       "type": "integer"
         |     }
         |   }
         | }
         |""".stripMargin)

    val transformedPlan = rule.apply(logicalRelation)
    assert(transformedPlan.isInstanceOf[DataSourceV2Relation])
  }

  test("Covering index should be applied when all columns are covered 2") {
    assertFlintQueryRewriter
      .withTable(StructType.fromDDL("name STRING, age INT"))
      .withProject("name", "age")
      .withIndex(
        new FlintSparkCoveringIndex(
          indexName = "test_index",
          tableName = testTable,
          indexedColumns = Map("name" -> "string", "age" -> "int")))
      .assertIndexUsed()
  }

  private def assertFlintQueryRewriter: AssertionHelper = new AssertionHelper

  class AssertionHelper {
    private var schema: StructType = _
    private var plan: LogicalPlan = _
    private var index: FlintSparkCoveringIndex = _

    def withTable(schema: StructType): AssertionHelper = {
      this.schema = schema
      val baseRelation = mock[BaseRelation]
      when(baseRelation.schema).thenReturn(schema)

      val table = mock[CatalogTable]
      when(table.qualifiedName).thenReturn(testTable)

      this.plan = LogicalRelation(baseRelation, table)
      this
    }

    def withProject(colNames: String*): AssertionHelper = {
      val output = colNames.map(name => AttributeReference(name, schema(name).dataType)())
      this.plan = Project(output, plan)
      this
    }

    def withIndex(index: FlintSparkCoveringIndex): AssertionHelper = {
      this.index = index
      when(flint.describeIndexes(anyString())).thenReturn(Seq(index))
      this
    }

    def assertIndexUsed(): AssertionHelper = {
      when(client.getIndexMetadata(anyString())).thenReturn(index.metadata())

      rule.apply(plan) should scanIndexOnly
      this
    }

    private def scanIndexOnly(): Matcher[LogicalPlan] = {
      Matcher { (plan: LogicalPlan) =>
        val result = plan.exists {
          case relation: DataSourceV2Relation =>
            relation.table.name() == index.name()
          case _ => false
        }

        MatchResult(
          result,
          "Plan does not scan index only as expected",
          "Plan scan index only as expected")
      }
    }
  }
}
