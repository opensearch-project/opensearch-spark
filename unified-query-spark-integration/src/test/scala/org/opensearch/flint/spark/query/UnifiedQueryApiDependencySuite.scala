/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query

import java.util.{List => JList, Locale, Map => JMap}

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.{Schema, Statistic, Statistics, Table}
import org.apache.calcite.schema.Schema.TableType
import org.apache.calcite.schema.impl.AbstractSchema
import org.apache.calcite.sql.`type`.SqlTypeName
import org.opensearch.sql.api.{UnifiedQueryContext, UnifiedQueryPlanner}
import org.opensearch.sql.api.transpiler.UnifiedQueryTranspiler
import org.opensearch.sql.executor.QueryType
import org.opensearch.sql.ppl.calcite.OpenSearchSparkSqlDialect
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Test suite to verify that the unified-query-api dependency is correctly
 * configured and can be used to plan and transpile PPL queries.
 *
 * This test validates:
 * - The dependency is resolved correctly from the OpenSearch CI snapshots repository
 * - Core classes from unified-query-api can be imported and instantiated
 * - PPL queries can be planned into Calcite RelNode logical plans
 * - Logical plans can be transpiled to Spark SQL using OpenSearchSparkSqlDialect
 */
class UnifiedQueryApiDependencySuite extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var context: UnifiedQueryContext = _
  private var planner: UnifiedQueryPlanner = _

  override def beforeEach(): Unit = {
    // Create a test schema with an employees table
    val testSchema = new AbstractSchema() {
      override protected def getTableMap: JMap[String, Table] = {
        JMap.of("employees", new SimpleTable())
      }
    }

    // Create unified query context with PPL language and test catalog
    context = UnifiedQueryContext.builder()
      .language(QueryType.PPL)
      .catalog("catalog", testSchema)
      .build()

    planner = new UnifiedQueryPlanner(context)
  }

  override def afterEach(): Unit = {
    if (context != null) {
      context.close()
    }
  }

  "UnifiedQueryPlanner" should "plan a simple PPL source query" in {
    val plan = planner.plan("source = catalog.employees")
    plan should not be null
  }

  it should "plan a PPL query with where clause" in {
    val plan = planner.plan("source = catalog.employees | where age > 30")
    plan should not be null
  }

  it should "plan a PPL query with fields command" in {
    val plan = planner.plan("source = catalog.employees | fields id, name")
    plan should not be null
  }

  it should "plan a PPL query with eval command" in {
    val plan = planner.plan("source = catalog.employees | eval doubled_age = age * 2")
    plan should not be null
  }

  "UnifiedQueryTranspiler" should "transpile PPL to Spark SQL" in {
    val plan = planner.plan("source = catalog.employees | where age > 30")

    val transpiler = UnifiedQueryTranspiler.builder()
      .dialect(OpenSearchSparkSqlDialect.DEFAULT)
      .build()

    val sql = transpiler.toSql(plan)
    sql should not be empty
    sql.toLowerCase(Locale.ROOT) should include("where")
  }

  it should "transpile PPL with fields to Spark SQL with SELECT" in {
    val plan = planner.plan("source = catalog.employees | fields id, name")

    val transpiler = UnifiedQueryTranspiler.builder()
      .dialect(OpenSearchSparkSqlDialect.DEFAULT)
      .build()

    val sql = transpiler.toSql(plan)
    sql should not be empty
    // The SQL should select specific columns
    sql.toLowerCase(Locale.ROOT) should (include("id") and include("name"))
  }

  /**
   * Simple table implementation for testing purposes.
   * Defines a schema with id, name, age, and department columns.
   */
  private class SimpleTable extends Table {
    override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
      typeFactory.builder()
        .add("id", SqlTypeName.INTEGER)
        .add("name", SqlTypeName.VARCHAR)
        .add("age", SqlTypeName.INTEGER)
        .add("department", SqlTypeName.VARCHAR)
        .build()
    }

    override def getStatistic: Statistic = Statistics.UNKNOWN

    override def getJdbcTableType: TableType = TableType.TABLE

    override def isRolledUp(column: String): Boolean = false

    override def rolledUpColumnValidInsideAgg(
        column: String,
        call: org.apache.calcite.sql.SqlCall,
        parent: org.apache.calcite.sql.SqlNode,
        config: org.apache.calcite.config.CalciteConnectionConfig): Boolean = false
  }
}
