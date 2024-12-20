/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import java.util
import java.util.Objects.requireNonNull

import org.scalatest.matchers.should.Matchers

import org.apache.calcite.adapter.java.AbstractQueryableTable
import org.apache.calcite.config.{CalciteConnectionConfig, Lex}
import org.apache.calcite.jdbc.{CalciteSchema, JavaTypeFactoryImpl}
import org.apache.calcite.linq4j.{Enumerable, Linq4j, QueryProvider, Queryable}
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.prepare.{CalciteCatalogReader, PlannerImpl}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.SqlDialect.DatabaseProduct
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.tools.{FrameworkConfig, Frameworks, Programs}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.PlanTest

class PPLSqlNodeTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  val t: AbstractTable = new AbstractQueryableTable(classOf[Integer]) {
    val enumerable: Enumerable[Integer] = Linq4j.asEnumerable(new util.ArrayList[Integer]())

    override def asQueryable[E](queryProvider: QueryProvider, schema: SchemaPlus, tableName: String): Queryable[E] = enumerable.asQueryable.asInstanceOf[Queryable[E]]

    override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
      val builder: RelDataTypeFactory.Builder = typeFactory.builder
      builder.add("a", SqlTypeName.INTEGER)
      builder.add("b", SqlTypeName.INTEGER)
      builder.add("c", SqlTypeName.INTEGER)
      for (i <- 0 until 3) {
        builder.add(s"c$i", SqlTypeName.INTEGER)
      }
      builder.build
    }
  }

  private def createCatalogReader = {
    val defaultSchema = requireNonNull(config.getDefaultSchema, "defaultSchema")
    val rootSchema = defaultSchema
    new CalciteCatalogReader(CalciteSchema.from(rootSchema), CalciteSchema.from(defaultSchema).path(null), typeFactory, CalciteConnectionConfig.DEFAULT)
  }

  val schema: SchemaPlus = Frameworks.createRootSchema(true)
  schema.add("table", t)
  val config: FrameworkConfig = Frameworks.newConfigBuilder
    .parserConfig(SqlParser.config.withLex(Lex.MYSQL))
    .defaultSchema(schema)
    .programs(Programs.ofRules(Programs.RULE_SET))
    .build
  val typeFactory = new JavaTypeFactoryImpl(config.getTypeSystem)
  val pplParser = new PPLParser()
  val planner = Frameworks.getPlanner(config)
  val cluster: RelOptCluster = RelOptCluster.create(requireNonNull(new VolcanoPlanner(config.getCostFactory, config.getContext), "planner"), new RexBuilder(typeFactory))
  val sqlToRelConverter = new SqlToRelConverter(planner.asInstanceOf[PlannerImpl], null, createCatalogReader, cluster, config.getConvertletTable, config.getSqlToRelConverterConfig)
  val relToSqlConverter = new RelToSqlConverter(DatabaseProduct.CALCITE.getDialect)
  val pplParserOld = new PPLSyntaxParser()

  test("test") {
    val sqlNode = pplParser.parseQuery("source=table | where a = 1| stats avg(b) as avg_b by c |  sort c | fields c, avg_b")
    val relNode = sqlToRelConverter.convertQuery(sqlNode, false, true)

    val sqlNode2 = planner.parse(sqlNode.toString())
    planner.validate(sqlNode2)
    val relNode2 = planner.rel(sqlNode2)
    val sqlNode3 = relToSqlConverter.visitRoot(relNode.rel).asStatement()

    // val relNode = planner.rel(sqlNode)
    // val osPlan = plan(pplParserOld, "source=t")
    //scalastyle:off
    println(sqlNode)
    println(relNode2)
    println(sqlNode3)
    // println(osPlan)
    //scalastyle:on
  }

}
