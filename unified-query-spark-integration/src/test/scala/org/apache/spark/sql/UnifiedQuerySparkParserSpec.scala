/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.mockito.ArgumentMatchers.anyString
import org.mockito.IdiomaticMockito
import org.mockito.Mockito._
import org.opensearch.flint.spark.query.{SparkSchema, UnifiedQuerySparkParser}
import org.opensearch.sql.api.UnifiedQueryContext
import org.opensearch.sql.executor.QueryType
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class UnifiedQuerySparkParserSpec
    extends AnyFunSpec
    with Matchers
    with IdiomaticMockito
    with BeforeAndAfterEach {

  private val testSchema = StructType(
    Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType)))

  private var sparkParser: ParserInterface = _
  private var parser: UnifiedQuerySparkParser = _
  private var mockPlan: LogicalPlan = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    sparkParser = mock[ParserInterface]
    mockPlan = mock[LogicalPlan]

    // Always return the same test schema
    val spark = mock[SparkSession]
    val table = mock[DataFrame]
    when(table.schema).thenReturn(testSchema)
    when(spark.table(anyString())).thenReturn(table)

    // Build context with all catalogs needed by tests
    val context = UnifiedQueryContext
      .builder()
      .language(QueryType.PPL)
      .defaultNamespace("spark_catalog.default")
      .catalog("spark_catalog", new SparkSchema(spark, "spark_catalog"))
      .catalog("iceberg_catalog", new SparkSchema(spark, "iceberg_catalog"))
      // Required by join test to simplify the SQL query text
      .setting("plugins.calcite.all_join_types.allowed", true)
      .setting("plugins.ppl.subsearch.maxout", 0)
      .setting("plugins.ppl.join.subsearch_maxout", 0)
      .build()
    parser = new UnifiedQuerySparkParser(context, sparkParser)
  }

  describe("PPL query parsing") {
    it("should transpile PPL query to Spark SQL") {
      val expectedSql = "SELECT *\nFROM `spark_catalog`.`default`.`test_table`"
      when(sparkParser.parsePlan(expectedSql)).thenReturn(mockPlan)

      parser.parsePlan("source=spark_catalog.default.test_table") shouldBe mockPlan
    }

    it("should fall back to Spark parser for non-PPL queries") {
      when(sparkParser.parsePlan("SELECT * FROM test_table")).thenReturn(mockPlan)

      parser.parsePlan("SELECT * FROM test_table") shouldBe mockPlan
    }

    it("should transpile PPL query with non-default catalog") {
      val expectedSql = "SELECT *\nFROM `iceberg_catalog`.`default`.`events`"
      when(sparkParser.parsePlan(expectedSql)).thenReturn(mockPlan)

      parser.parsePlan("source=iceberg_catalog.default.events") shouldBe mockPlan
    }

    it("should transpile PPL query with multiple catalog") {
      val expectedSql =
        """SELECT `customers`.`name`, `orders`.`name` `r.name`
          |FROM `spark_catalog`.`default`.`customers`
          |INNER JOIN `iceberg_catalog`.`default`.`orders` ON `customers`.`id` = `orders`.`id`""".stripMargin
      when(sparkParser.parsePlan(expectedSql)).thenReturn(mockPlan)

      val pplQuery =
        """source=spark_catalog.default.customers
          | join left=l right=r ON l.id = r.id iceberg_catalog.default.orders
          | fields l.name, r.name""".stripMargin('\n')
      parser.parsePlan(pplQuery) shouldBe mockPlan
    }
  }

  describe("delegated methods") {
    it("should delegate parseExpression to Spark parser") {
      val expr = Literal(1)
      when(sparkParser.parseExpression("expr")).thenReturn(expr)

      parser.parseExpression("expr") shouldBe expr
    }

    it("should delegate parseTableIdentifier to Spark parser") {
      val tableId = TableIdentifier("t")
      when(sparkParser.parseTableIdentifier("table")).thenReturn(tableId)

      parser.parseTableIdentifier("table") shouldBe tableId
    }

    it("should delegate parseFunctionIdentifier to Spark parser") {
      val funcId = FunctionIdentifier("f")
      when(sparkParser.parseFunctionIdentifier("func")).thenReturn(funcId)

      parser.parseFunctionIdentifier("func") shouldBe funcId
    }

    it("should delegate parseMultipartIdentifier to Spark parser") {
      val parts = Seq("a", "b")
      when(sparkParser.parseMultipartIdentifier("multi")).thenReturn(parts)

      parser.parseMultipartIdentifier("multi") shouldBe parts
    }

    it("should delegate parseTableSchema to Spark parser") {
      when(sparkParser.parseTableSchema("schema")).thenReturn(testSchema)

      parser.parseTableSchema("schema") shouldBe testSchema
    }

    it("should delegate parseDataType to Spark parser") {
      when(sparkParser.parseDataType("INT")).thenReturn(IntegerType)

      parser.parseDataType("INT") shouldBe IntegerType
    }

    it("should delegate parseQuery to Spark parser") {
      when(sparkParser.parseQuery("query")).thenReturn(mockPlan)

      parser.parseQuery("query") shouldBe mockPlan
    }
  }
}
