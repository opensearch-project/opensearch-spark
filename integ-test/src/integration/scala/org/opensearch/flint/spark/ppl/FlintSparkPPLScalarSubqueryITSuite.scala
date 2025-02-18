/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, EqualTo, GreaterThan, Literal, Or, ScalarSubquery, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLScalarSubqueryITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val outerTable = "spark_catalog.default.flint_ppl_test1"
  private val innerTable = "spark_catalog.default.flint_ppl_test2"
  private val nestedInnerTable = "spark_catalog.default.flint_ppl_test3"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPeopleTable(outerTable)
    sql(s"""
           | INSERT INTO $outerTable
           | VALUES (1006, 'Tommy', 'Teacher', 'USA', 30000)
           | """.stripMargin)
    createWorkInformationTable(innerTable)
    createOccupationTable(nestedInnerTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test uncorrelated scalar subquery in select") {
    val frame = sql(s"""
                       | source = $outerTable
                       | | eval count_dept = [
                       |     source = $innerTable | stats count(department)
                       |   ]
                       | | fields name, count_dept
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row("Jake", 5),
      Row("Hello", 5),
      Row("John", 5),
      Row("David", 5),
      Row("David", 5),
      Row("Jane", 5),
      Row("Tommy", 5))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(
          Seq("COUNT"),
          Seq(UnresolvedAttribute("department")),
          isDistinct = false),
        "count(department)")())
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, inner)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val evalProjectList = Seq(UnresolvedStar(None), Alias(scalarSubqueryExpr, "count_dept")())
    val evalProject = Project(evalProjectList, outer)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("name"), UnresolvedAttribute("count_dept")), evalProject)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test uncorrelated scalar subquery in expression in select") {
    val frame = sql(s"""
                       | source = $outerTable
                       | | eval count_dept_plus = [
                       |     source = $innerTable | stats count(department)
                       |   ] + 10
                       | | fields name, count_dept_plus
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row("Jake", 15),
      Row("Hello", 15),
      Row("John", 15),
      Row("David", 15),
      Row("David", 15),
      Row("Jane", 15),
      Row("Tommy", 15))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(
          Seq("COUNT"),
          Seq(UnresolvedAttribute("department")),
          isDistinct = false),
        "count(department)")())
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, inner)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val scalarSubqueryPlus =
      UnresolvedFunction(Seq("+"), Seq(scalarSubqueryExpr, Literal(10)), isDistinct = false)
    val evalProjectList =
      Seq(UnresolvedStar(None), Alias(scalarSubqueryPlus, "count_dept_plus")())
    val evalProject = Project(evalProjectList, outer)
    val expectedPlan =
      Project(
        Seq(UnresolvedAttribute("name"), UnresolvedAttribute("count_dept_plus")),
        evalProject)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test uncorrelated scalar subquery in select and where") {
    val frame = sql(s"""
                       | source = $outerTable
                       | | where id > [
                       |     source = $innerTable | stats count(department)
                       |   ] + 999
                       | | eval count_dept = [
                       |     source = $innerTable | stats count(department)
                       |   ]
                       | | fields name, count_dept
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Jane", 5), Row("Tommy", 5))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val countAgg = Seq(
      Alias(
        UnresolvedFunction(
          Seq("COUNT"),
          Seq(UnresolvedAttribute("department")),
          isDistinct = false),
        "count(department)")())
    val countAggPlan = Aggregate(Seq(), countAgg, inner)
    val countScalarSubqueryExpr = ScalarSubquery(countAggPlan)
    val plusScalarSubquery =
      UnresolvedFunction(Seq("+"), Seq(countScalarSubqueryExpr, Literal(999)), isDistinct = false)
    val filter = Filter(GreaterThan(UnresolvedAttribute("id"), plusScalarSubquery), outer)
    val evalProjectList =
      Seq(UnresolvedStar(None), Alias(countScalarSubqueryExpr, "count_dept")())
    val evalProject = Project(evalProjectList, filter)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("name"), UnresolvedAttribute("count_dept")), evalProject)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test uncorrelated scalar subquery in select and from with filter") {
    val frame = sql(s"""
                       | source = $outerTable id > [ source = $innerTable | stats count(department) ] + 999
                       | | eval count_dept = [
                       |     source = $innerTable | stats count(department)
                       |   ]
                       | | fields name, count_dept
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Jane", 5), Row("Tommy", 5))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val countAgg = Seq(
      Alias(
        UnresolvedFunction(
          Seq("COUNT"),
          Seq(UnresolvedAttribute("department")),
          isDistinct = false),
        "count(department)")())
    val countAggPlan = Aggregate(Seq(), countAgg, inner)
    val countScalarSubqueryExpr = ScalarSubquery(countAggPlan)
    val plusScalarSubquery =
      UnresolvedFunction(Seq("+"), Seq(countScalarSubqueryExpr, Literal(999)), isDistinct = false)
    val filter = Filter(GreaterThan(UnresolvedAttribute("id"), plusScalarSubquery), outer)
    val evalProjectList =
      Seq(UnresolvedStar(None), Alias(countScalarSubqueryExpr, "count_dept")())
    val evalProject = Project(evalProjectList, filter)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("name"), UnresolvedAttribute("count_dept")), evalProject)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test correlated scalar subquery in select") {
    val frame = sql(s"""
                       | source = $outerTable
                       | | eval count_dept = [
                       |     source = $innerTable
                       |     | where id = uid | stats count(department)
                       |   ]
                       | | fields id, name, count_dept
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", 1),
      Row(1001, "Hello", 0),
      Row(1002, "John", 1),
      Row(1003, "David", 1),
      Row(1004, "David", 0),
      Row(1005, "Jane", 1),
      Row(1006, "Tommy", 1))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(
          Seq("COUNT"),
          Seq(UnresolvedAttribute("department")),
          isDistinct = false),
        "count(department)")())
    val filter = Filter(EqualTo(UnresolvedAttribute("id"), UnresolvedAttribute("uid")), inner)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, filter)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val evalProjectList = Seq(UnresolvedStar(None), Alias(scalarSubqueryExpr, "count_dept")())
    val evalProject = Project(evalProjectList, outer)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("id"),
          UnresolvedAttribute("name"),
          UnresolvedAttribute("count_dept")),
        evalProject)

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test correlated scalar subquery in select with non-equal") {
    val frame = sql(s"""
                       | source = $outerTable
                       | | eval count_dept = [
                       |     source = $innerTable | where id > uid | stats count(department)
                       |   ]
                       | | fields id, name, count_dept
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", 0),
      Row(1001, "Hello", 1),
      Row(1002, "John", 1),
      Row(1003, "David", 2),
      Row(1004, "David", 3),
      Row(1005, "Jane", 3),
      Row(1006, "Tommy", 4))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(
          Seq("COUNT"),
          Seq(UnresolvedAttribute("department")),
          isDistinct = false),
        "count(department)")())
    val filter = Filter(GreaterThan(UnresolvedAttribute("id"), UnresolvedAttribute("uid")), inner)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, filter)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val evalProjectList = Seq(UnresolvedStar(None), Alias(scalarSubqueryExpr, "count_dept")())
    val evalProject = Project(evalProjectList, outer)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("id"),
          UnresolvedAttribute("name"),
          UnresolvedAttribute("count_dept")),
        evalProject)

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test correlated scalar subquery in where") {
    val frame = sql(s"""
             | source = $outerTable
             | | where id = [
             |     source = $innerTable | where id = uid | stats max(uid)
             |   ]
             | | fields id, name
             | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake"),
      Row(1002, "John"),
      Row(1003, "David"),
      Row(1005, "Jane"),
      Row(1006, "Tommy"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("uid")), isDistinct = false),
        "max(uid)")())
    val innerFilter =
      Filter(EqualTo(UnresolvedAttribute("id"), UnresolvedAttribute("uid")), inner)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, innerFilter)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val outerFilter = Filter(EqualTo(UnresolvedAttribute("id"), scalarSubqueryExpr), outer)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("id"), UnresolvedAttribute("name")), outerFilter)

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }
  test("test correlated scalar subquery in from with filter") {
    val frame = sql(s"""
                       | source = $outerTable id = [ source = $innerTable | where id = uid | stats max(uid) ]
                       | | fields id, name
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake"),
      Row(1002, "John"),
      Row(1003, "David"),
      Row(1005, "Jane"),
      Row(1006, "Tommy"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("uid")), isDistinct = false),
        "max(uid)")())
    val innerFilter =
      Filter(EqualTo(UnresolvedAttribute("id"), UnresolvedAttribute("uid")), inner)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, innerFilter)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val outerFilter = Filter(EqualTo(UnresolvedAttribute("id"), scalarSubqueryExpr), outer)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("id"), UnresolvedAttribute("name")), outerFilter)

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test disjunctive correlated scalar subquery") {
    val frame = sql(s"""
                       | source = $outerTable
                       | | where [
                       |     source = $innerTable | where id = uid OR uid = 1010 | stats count()
                       |   ] > 0
                       | | fields id, name
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake"),
      Row(1002, "John"),
      Row(1003, "David"),
      Row(1005, "Jane"),
      Row(1006, "Tommy"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
        "count()")())
    val innerFilter =
      Filter(
        Or(
          EqualTo(UnresolvedAttribute("id"), UnresolvedAttribute("uid")),
          EqualTo(UnresolvedAttribute("uid"), Literal(1010))),
        inner)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, innerFilter)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val outerFilter = Filter(GreaterThan(scalarSubqueryExpr, Literal(0)), outer)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("id"), UnresolvedAttribute("name")), outerFilter)

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test two scalar subqueries in OR") {
    val frame = sql(s"""
                       | source = $outerTable
                       | | where id = [
                       |     source = $innerTable | sort uid | stats max(uid)
                       |   ] OR id = [
                       |     source = $innerTable | sort uid | where department = 'DATA' | stats min(uid)
                       |   ]
                       | | fields id, name
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(1002, "John"), Row(1006, "Tommy"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val maxExpr =
      UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("uid")), isDistinct = false)
    val minExpr =
      UnresolvedFunction(Seq("MIN"), Seq(UnresolvedAttribute("uid")), isDistinct = false)
    val maxAgg = Seq(Alias(maxExpr, "max(uid)")())
    val minAgg = Seq(Alias(minExpr, "min(uid)")())
    val subquery1 =
      Sort(Seq(SortOrder(UnresolvedAttribute("uid"), Ascending)), global = true, inner)
    val subquery2 =
      Sort(Seq(SortOrder(UnresolvedAttribute("uid"), Ascending)), global = true, inner)
    val maxAggPlan = Aggregate(Seq(), maxAgg, subquery1)
    val minAggPlan =
      Aggregate(
        Seq(),
        minAgg,
        Filter(EqualTo(UnresolvedAttribute("department"), Literal("DATA")), subquery2))
    val maxScalarSubqueryExpr = ScalarSubquery(maxAggPlan)
    val minScalarSubqueryExpr = ScalarSubquery(minAggPlan)
    val filterOr = Filter(
      Or(
        EqualTo(UnresolvedAttribute("id"), maxScalarSubqueryExpr),
        EqualTo(UnresolvedAttribute("id"), minScalarSubqueryExpr)),
      outer)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("id"), UnresolvedAttribute("name")), filterOr)

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test nested scalar subquery") {
    val frame = sql(s"""
                       | source = $outerTable
                       | | where id = [
                       |     source = $innerTable
                       |     | where uid = [
                       |         source = $nestedInnerTable
                       |         | stats min(salary)
                       |       ] + 1000
                       |     | sort department
                       |     | stats max(uid)
                       |   ]
                       | | fields id, name
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(1000, "Jake"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test nested scalar subquery with table alias") {
    val frame = sql(s"""
                       | source = $outerTable as o
                       | | where id = [
                       |     source = $innerTable as i
                       |     | where uid = [
                       |         source = $nestedInnerTable as n
                       |         | stats min(n.salary)
                       |       ] + 1000
                       |     | sort i.department
                       |     | stats max(i.uid)
                       |   ]
                       | | fields o.id, o.name
                       | """.stripMargin)
    val expectedResults: Array[Row] = Array(Row(1000, "Jake"))
    assertSameRows(expectedResults, frame)
  }

  test("test correlated scalar subquery with table alias") {
    val frame = sql(s"""
                       | source = $outerTable as o
                       | | where id = [
                       |     source = $innerTable as i | where o.id = i.uid | stats max(i.uid)
                       |   ]
                       | | fields o.id, o.name
                       | """.stripMargin)
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake"),
      Row(1002, "John"),
      Row(1003, "David"),
      Row(1005, "Jane"),
      Row(1006, "Tommy"))
    assertSameRows(expectedResults, frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer =
      SubqueryAlias("o", UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1")))
    val inner =
      SubqueryAlias("i", UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2")))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("i.uid")), isDistinct = false),
        "max(i.uid)")())
    val innerFilter =
      Filter(EqualTo(UnresolvedAttribute("o.id"), UnresolvedAttribute("i.uid")), inner)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, innerFilter)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val outerFilter = Filter(EqualTo(UnresolvedAttribute("id"), scalarSubqueryExpr), outer)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("o.id"), UnresolvedAttribute("o.name")), outerFilter)

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test uncorrelated scalar subquery with table alias") {
    val frame = sql(s"""
                       | source = $outerTable as o
                       | | eval max_uid = [
                       |     source = $innerTable as i | where i.department = 'DATA' | stats max(i.uid)
                       |   ]
                       | | fields o.id, o.name, max_uid
                       | """.stripMargin)
    val expectedResults: Array[Row] = Array(
      Row(1000, "Jake", 1005),
      Row(1001, "Hello", 1005),
      Row(1002, "John", 1005),
      Row(1003, "David", 1005),
      Row(1004, "David", 1005),
      Row(1005, "Jane", 1005),
      Row(1006, "Tommy", 1005))
    assertSameRows(expectedResults, frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val outer =
      SubqueryAlias("o", UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1")))
    val inner =
      SubqueryAlias("i", UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2")))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("i.uid")), isDistinct = false),
        "max(i.uid)")())
    val innerFilter =
      Filter(EqualTo(UnresolvedAttribute("i.department"), Literal("DATA")), inner)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, innerFilter)
    val scalarSubqueryExpr = Alias(ScalarSubquery(aggregatePlan), "max_uid")()
    val outerFilter = Project(Seq(UnresolvedStar(None), scalarSubqueryExpr), outer)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("o.id"),
          UnresolvedAttribute("o.name"),
          UnresolvedAttribute("max_uid")),
        outerFilter)

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }
}
