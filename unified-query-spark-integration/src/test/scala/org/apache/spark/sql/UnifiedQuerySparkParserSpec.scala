/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.mockito.Answers.RETURNS_DEEP_STUBS
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito._
import org.opensearch.flint.spark.query.UnifiedQuerySparkParser
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Unit tests for UnifiedQuerySparkParser verifying PPL to Spark SQL transpilation. Uses mocked
 * SparkSession to avoid heavyweight Spark initialization.
 */
class UnifiedQuerySparkParserSpec
    extends AnyFunSpec
    with Matchers
    with MockitoSugar
    with BeforeAndAfterEach {

  private val testSchema = StructType(
    Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType)))

  private var spark: SparkSession = _
  private var sparkParser: ParserInterface = _
  private var parser: UnifiedQuerySparkParser = _
  private var mockPlan: LogicalPlan = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Deep stub SparkSession for cleaner mock setup
    spark = mock[SparkSession](RETURNS_DEEP_STUBS)
    when(spark.sessionState.catalogManager.listCatalogs(None)).thenReturn(Seq("spark_catalog"))
    when(spark.catalog.currentCatalog).thenReturn("spark_catalog")
    when(spark.catalog.currentDatabase).thenReturn("default")
    when(spark.table(anyString()).schema).thenReturn(testSchema)

    sparkParser = mock[ParserInterface]
    mockPlan = mock[LogicalPlan]
    parser = UnifiedQuerySparkParser(spark, sparkParser)
  }

  describe("PPL query parsing") {
    it("should transpile PPL query to Spark SQL") {
      val expectedSql = "SELECT *\nFROM `spark_catalog`.`default`.`test_table`"
      when(sparkParser.parsePlan(expectedSql)).thenReturn(mockPlan)

      parser.parsePlan("source=spark_catalog.default.test_table") shouldBe mockPlan
      verify(sparkParser).parsePlan(expectedSql)
    }

    it("should fall back to Spark parser for non-PPL queries") {
      when(sparkParser.parsePlan("SELECT * FROM test_table")).thenReturn(mockPlan)

      parser.parsePlan("SELECT * FROM test_table") shouldBe mockPlan
      verify(sparkParser).parsePlan("SELECT * FROM test_table")
    }
  }

  describe("multi-catalog query resolution") {
    it("should resolve queries with non-default catalog name") {
      when(spark.sessionState.catalogManager.listCatalogs(None))
        .thenReturn(Seq("iceberg_catalog"))
      when(spark.catalog.currentCatalog).thenReturn("iceberg_catalog")
      when(spark.catalog.currentDatabase).thenReturn("analytics")

      val customParser = UnifiedQuerySparkParser(spark, sparkParser)
      val expectedSql = "SELECT *\nFROM `iceberg_catalog`.`analytics`.`events`"
      when(sparkParser.parsePlan(expectedSql)).thenReturn(mockPlan)

      customParser.parsePlan("source=iceberg_catalog.analytics.events") shouldBe mockPlan
      verify(sparkParser).parsePlan(expectedSql)
    }

    it("should resolve queries across multiple registered catalogs") {
      when(spark.sessionState.catalogManager.listCatalogs(None))
        .thenReturn(Seq("spark_catalog", "iceberg_catalog", "delta_catalog"))

      val multiParser = UnifiedQuerySparkParser(spark, sparkParser)
      val expectedSql = "SELECT *\nFROM `iceberg_catalog`.`warehouse`.`orders`"
      when(sparkParser.parsePlan(expectedSql)).thenReturn(mockPlan)

      multiParser.parsePlan("source=iceberg_catalog.warehouse.orders") shouldBe mockPlan
      verify(sparkParser).parsePlan(expectedSql)
    }
  }

  describe("delegated methods") {
    it("should delegate parseExpression to Spark parser") {
      val expr = Literal(1)
      when(sparkParser.parseExpression("expr")).thenReturn(expr)

      parser.parseExpression("expr") shouldBe expr
      verify(sparkParser).parseExpression("expr")
    }

    it("should delegate parseTableIdentifier to Spark parser") {
      val tableId = TableIdentifier("t")
      when(sparkParser.parseTableIdentifier("table")).thenReturn(tableId)

      parser.parseTableIdentifier("table") shouldBe tableId
      verify(sparkParser).parseTableIdentifier("table")
    }

    it("should delegate parseFunctionIdentifier to Spark parser") {
      val funcId = FunctionIdentifier("f")
      when(sparkParser.parseFunctionIdentifier("func")).thenReturn(funcId)

      parser.parseFunctionIdentifier("func") shouldBe funcId
      verify(sparkParser).parseFunctionIdentifier("func")
    }

    it("should delegate parseMultipartIdentifier to Spark parser") {
      val parts = Seq("a", "b")
      when(sparkParser.parseMultipartIdentifier("multi")).thenReturn(parts)

      parser.parseMultipartIdentifier("multi") shouldBe parts
      verify(sparkParser).parseMultipartIdentifier("multi")
    }

    it("should delegate parseTableSchema to Spark parser") {
      when(sparkParser.parseTableSchema("schema")).thenReturn(testSchema)

      parser.parseTableSchema("schema") shouldBe testSchema
      verify(sparkParser).parseTableSchema("schema")
    }

    it("should delegate parseDataType to Spark parser") {
      when(sparkParser.parseDataType("INT")).thenReturn(IntegerType)

      parser.parseDataType("INT") shouldBe IntegerType
      verify(sparkParser).parseDataType("INT")
    }

    it("should delegate parseQuery to Spark parser") {
      when(sparkParser.parseQuery("query")).thenReturn(mockPlan)

      parser.parseQuery("query") shouldBe mockPlan
      verify(sparkParser).parseQuery("query")
    }
  }
}
