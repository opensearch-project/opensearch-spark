/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import scala.collection.JavaConverters._

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mockStatic, when, RETURNS_DEEP_STUBS}
import org.opensearch.client.opensearch.core.pit.{CreatePitRequest, CreatePitResponse}
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState.{ACTIVE, DELETED, IndexState}
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder, FlintOptions, IRestHighLevelClient}
import org.opensearch.flint.core.storage.OpenSearchClientUtils
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
  private val testTable2 = "spark_catalog.default.apply_covering_index_test_2"

  /** Mock FlintClient to avoid looking for real OpenSearch cluster */
  private val clientBuilder = mockStatic(classOf[FlintClientBuilder])
  private val client = mock[FlintClient](RETURNS_DEEP_STUBS)

  /** Mock IRestHighLevelClient to avoid looking for real OpenSearch cluster */
  private val clientUtils = mockStatic(classOf[OpenSearchClientUtils])
  private val openSearchClient = mock[IRestHighLevelClient](RETURNS_DEEP_STUBS)
  private val pitResponse = mock[CreatePitResponse]

  /** Mock FlintSpark which is required by the rule. Deep stub required to replace spark val. */
  private val flint = mock[FlintSpark](RETURNS_DEEP_STUBS)

  /** Instantiate the rule once for all tests */
  private val rule = new ApplyFlintSparkCoveringIndex(flint)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE TABLE $testTable (name STRING, age INT) USING JSON")
    sql(s"CREATE TABLE $testTable2 (name STRING) USING JSON")
    sql(s"""
         | INSERT INTO $testTable
         | VALUES
         |  ('A', 10), ('B', 15), ('C', 20), ('D', 25), ('E', 30),
         |  ('F', 35), ('G', 40), ('H', 45), ('I', 50), ('J', 55)
         | """.stripMargin)

    // Mock static create method in FlintClientBuilder used by Flint data source
    clientBuilder
      .when(() => FlintClientBuilder.build(any(classOf[FlintOptions])))
      .thenReturn(client)
    when(flint.spark).thenReturn(spark)
    // Mock static
    clientUtils
      .when(() => OpenSearchClientUtils.createClient(any(classOf[FlintOptions])))
      .thenReturn(openSearchClient)
  }

  override protected def afterAll(): Unit = {
    sql(s"DROP TABLE $testTable")
    clientBuilder.close()
    super.afterAll()
  }

  test("should not apply if no covering index present") {
    assertFlintQueryRewriter
      .withQuery(s"SELECT name, age FROM $testTable")
      .assertIndexNotUsed(testTable)
  }

  // Comprehensive test by cartesian product of the following condition
  private val conditions = Seq(
    null,
    "age = 20",
    "age > 20",
    "age >= 20",
    "age < 20",
    "age <= 20",
    "age = 50",
    "age > 50",
    "age >= 50",
    "age < 50",
    "age <= 50",
    "age > 20 AND age < 50",
    "age >= 20 AND age < 50",
    "age > 20 AND age <= 50",
    "age >=20 AND age <= 50")
  (for {
    indexFilter <- conditions
    queryFilter <- conditions
  } yield (indexFilter, queryFilter)).distinct
    .foreach { case (indexFilter, queryFilter) =>
      test(s"apply partial covering index with [$indexFilter] to query filter [$queryFilter]") {
        def queryWithFilter(condition: String): String =
          Option(condition) match {
            case None => s"SELECT name FROM $testTable"
            case Some(cond) => s"SELECT name FROM $testTable WHERE $cond"
          }

        // Expect index applied if query result is subset of index data (index filter result)
        val queryData = sql(queryWithFilter(queryFilter)).collect().toSet
        val indexData = sql(queryWithFilter(indexFilter)).collect().toSet
        val expectedResult = queryData.subsetOf(indexData)

        val assertion = assertFlintQueryRewriter
          .withQuery(queryWithFilter(queryFilter))
          .withIndex(
            new FlintSparkCoveringIndex(
              indexName = "partial",
              tableName = testTable,
              indexedColumns = Map("name" -> "string", "age" -> "int"),
              filterCondition = Option(indexFilter)))

        if (expectedResult) {
          assertion.assertIndexUsed(getFlintIndexName("partial", testTable))
        } else {
          assertion.assertIndexNotUsed(testTable)
        }
      }
    }

  test("should not apply if covering index with disjunction filtering condition") {
    assertFlintQueryRewriter
      .withQuery(s"SELECT name FROM $testTable WHERE name = 'A' AND age > 30")
      .withIndex(
        new FlintSparkCoveringIndex(
          indexName = "partial",
          tableName = testTable,
          indexedColumns = Map("name" -> "string", "age" -> "int"),
          filterCondition = Some("name = 'A' OR age > 30")))
      .assertIndexNotUsed(testTable)
  }

  test("should not apply if covering index is logically deleted") {
    assertFlintQueryRewriter
      .withQuery(s"SELECT name FROM $testTable")
      .withIndex(
        new FlintSparkCoveringIndex(
          indexName = "name",
          tableName = testTable,
          indexedColumns = Map("name" -> "string")),
        DELETED)
      .assertIndexNotUsed(testTable)
  }

  // Covering index doesn't cover column age
  Seq(
    s"SELECT * FROM $testTable",
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
        .assertIndexNotUsed(testTable)
    }
  }

  // Covering index covers all columns
  Seq(
    s"SELECT * FROM $testTable",
    s"SELECT name, age FROM $testTable",
    s"SELECT age, name FROM $testTable",
    s"SELECT name FROM $testTable WHERE age = 30",
    s"SELECT name FROM $testTable WHERE name = 'A' AND age = 30",
    s"SELECT name FROM $testTable WHERE name = 'A' OR age = 30",
    s"SELECT SUBSTR(name, 1) FROM $testTable WHERE ABS(age) = 30",
    s"SELECT COUNT(*) FROM $testTable GROUP BY age",
    s"SELECT name, COUNT(*) FROM $testTable WHERE age > 30 GROUP BY name",
    s"SELECT age, COUNT(*) AS cnt FROM $testTable GROUP BY age ORDER BY cnt").foreach { query =>
    test(s"should apply if all columns are covered in $query") {
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

  test(s"should apply if one table is covered in join query") {
    assertFlintQueryRewriter
      .withQuery(s"""
           | SELECT t1.name, t1.age
           | FROM $testTable AS t1
           | JOIN $testTable2 AS t2
           | ON t1.name = t2.name
           |""".stripMargin)
      .withIndex(
        new FlintSparkCoveringIndex(
          indexName = "all",
          tableName = testTable,
          indexedColumns = Map("name" -> "string", "age" -> "int")))
      .assertIndexUsed(getFlintIndexName("all", testTable))
      .assertIndexNotUsed(testTable2)
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
      this.plan = sql(query).queryExecution.optimizedPlan
      this
    }

    def withIndex(index: FlintSparkCoveringIndex, state: IndexState = ACTIVE): AssertionHelper = {
      this.indexes = indexes :+
        index.copy(latestLogEntry = Some(
          new FlintMetadataLogEntry(
            "id",
            0,
            state,
            Map("seqNo" -> 0, "primaryTerm" -> 0),
            "",
            Map("dataSourceName" -> "dataSource"))))
      this
    }

    def assertIndexUsed(expectedIndexName: String): AssertionHelper = {
      rewritePlan should scanIndexOnly(expectedIndexName)
      this
    }

    def assertIndexNotUsed(expectedTableName: String): AssertionHelper = {
      rewritePlan should scanSourceTable(expectedTableName)
      this
    }

    private def rewritePlan: LogicalPlan = {
      // Assume all mock indexes are on test table
      when(flint.describeIndexes(any[String])).thenAnswer(invocation => {
        val indexName = invocation.getArgument(0).asInstanceOf[String]
        if (indexName == getFlintIndexName("*", testTable)) {
          indexes
        } else {
          Seq.empty
        }
      })

      indexes.foreach { index =>
        when(client.getAllIndexMetadata(index.name()))
          .thenReturn(Map.apply(index.name() -> index.metadata()).asJava)
        when(openSearchClient.createPit(any[CreatePitRequest]))
          .thenReturn(pitResponse)
        when(pitResponse.pitId()).thenReturn("")
      }
      rule.apply(plan)
    }

    private def scanSourceTable(expectedTableName: String): Matcher[LogicalPlan] = {
      Matcher { (plan: LogicalPlan) =>
        val result = plan.exists {
          case LogicalRelation(_, _, Some(table), _) =>
            // Table name in logical relation doesn't have catalog name
            table.qualifiedName == expectedTableName.split('.').drop(1).mkString(".")
          case _ => false
        }

        MatchResult(
          result,
          s"Plan does not scan table $expectedTableName",
          s"Plan scans table $expectedTableName as expected")
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
