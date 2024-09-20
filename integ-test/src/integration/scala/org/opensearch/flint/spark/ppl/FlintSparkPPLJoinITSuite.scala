/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, Divide, EqualTo, Floor, LessThan, Literal, Multiply, Or, SortOrder}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, JoinHint, LogicalPlan, Project, Sort, SubqueryAlias}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLJoinITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable1 = "spark_catalog.default.flint_ppl_test1"
  private val testTable2 = "spark_catalog.default.flint_ppl_test2"
  private val testTable3 = "spark_catalog.default.flint_ppl_test3"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create test tables
    createPartitionedStateCountryTable(testTable1)
    // Update data insertion
    sql(s"""
           | INSERT INTO $testTable1
           | PARTITION (year=2023, month=4)
           | VALUES ('Jim', 27,  'B.C', 'Canada'),
           |        ('Peter', 57,  'B.C', 'Canada'),
           |        ('Rick', 70,  'B.C', 'Canada'),
           |        ('David', 40,  'Washington', 'USA')
           | """.stripMargin)

    createOccupationTable(testTable2)
    createHobbiesTable(testTable3)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test join on one join condition and filters") {
    val frame = sql(s"""
         | source = $testTable1
         | | inner join left=a, right=b
         |     ON a.name = b.name AND a.year = 2023 AND a.month = 4 AND b.year = 2023 AND b.month = 4
         |     $testTable2
         | | fields a.name, a.age, a.state, a.country, b.occupation, b.country, b.salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, "California", "USA", "Engineer", "England", 100000),
      Row("Hello", 30, "New York", "USA", "Artist", "USA", 70000),
      Row("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000),
      Row("David", 40, "Washington", "USA", "Unemployed", "Canada", 0),
      Row("David", 40, "Washington", "USA", "Doctor", "USA", 120000),
      Row("Jane", 20, "Quebec", "Canada", "Scientist", "Canada", 90000))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val plan1 = SubqueryAlias("a", table1)
    val plan2 = SubqueryAlias("b", table2)

    val joinCondition =
      And(
        And(
          And(
            And(
              EqualTo(Literal(4), UnresolvedAttribute("a.month")),
              EqualTo(Literal(2023), UnresolvedAttribute("b.year"))),
            EqualTo(UnresolvedAttribute("a.name"), UnresolvedAttribute("b.name"))),
          EqualTo(UnresolvedAttribute("b.month"), Literal(4))),
        EqualTo(Literal(2023), UnresolvedAttribute("a.year")))
    val joinPlan = Join(plan1, plan2, Inner, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(
      Seq(
        UnresolvedAttribute("a.name"),
        UnresolvedAttribute("a.age"),
        UnresolvedAttribute("a.state"),
        UnresolvedAttribute("a.country"),
        UnresolvedAttribute("b.occupation"),
        UnresolvedAttribute("b.country"),
        UnresolvedAttribute("b.salary")),
      joinPlan)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test join on two join conditions and filters") {
    val frame = sql(s"""
         | source = $testTable1
         | | inner join left=a, right=b
         |     ON a.name = b.name AND a.country = b.country AND a.year = 2023 AND a.month = 4 AND b.year = 2023 AND b.month = 4
         |     $testTable2
         | | fields a.name, a.age, a.state, a.country, b.occupation, b.country, b.salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("David", 40, "Washington", "USA", "Doctor", "USA", 120000),
      Row("Hello", 30, "New York", "USA", "Artist", "USA", 70000),
      Row("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000),
      Row("Jane", 20, "Quebec", "Canada", "Scientist", "Canada", 90000))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val plan1 = SubqueryAlias("a", table1)
    val plan2 = SubqueryAlias("b", table2)

    val joinCondition =
      And(
        And(
          And(
            And(
              And(
                EqualTo(Literal(4), UnresolvedAttribute("a.month")),
                EqualTo(Literal(2023), UnresolvedAttribute("b.year"))),
              EqualTo(UnresolvedAttribute("a.country"), UnresolvedAttribute("b.country"))),
            EqualTo(UnresolvedAttribute("a.name"), UnresolvedAttribute("b.name"))),
          EqualTo(UnresolvedAttribute("b.month"), Literal(4))),
        EqualTo(Literal(2023), UnresolvedAttribute("a.year")))
    val joinPlan = Join(plan1, plan2, Inner, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(
      Seq(
        UnresolvedAttribute("a.name"),
        UnresolvedAttribute("a.age"),
        UnresolvedAttribute("a.state"),
        UnresolvedAttribute("a.country"),
        UnresolvedAttribute("b.occupation"),
        UnresolvedAttribute("b.country"),
        UnresolvedAttribute("b.salary")),
      joinPlan)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test join with two columns and disjoint filters") {
    val frame = sql(s"""
         | source = $testTable1
         | | inner join left=a, right=b
         |     ON a.name = b.name AND a.country = b.country AND a.year = 2023 AND a.month = 4 AND b.salary > 100000
         |     $testTable2
         | | fields a.name, a.age, a.state, a.country, b.occupation, b.country, b.salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("David", 40, "Washington", "USA", "Doctor", "USA", 120000),
      Row("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val plan1 = SubqueryAlias("a", table1)
    val plan2 = SubqueryAlias("b", table2)

    val joinCondition =
      And(
        And(
          And(
            And(
              LessThan(Literal(100000), UnresolvedAttribute("b.salary")),
              EqualTo(Literal(4), UnresolvedAttribute("a.month"))),
            EqualTo(UnresolvedAttribute("a.country"), UnresolvedAttribute("b.country"))),
          EqualTo(UnresolvedAttribute("a.name"), UnresolvedAttribute("b.name"))),
        EqualTo(Literal(2023), UnresolvedAttribute("a.year")))
    val joinPlan = Join(plan1, plan2, Inner, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(
      Seq(
        UnresolvedAttribute("a.name"),
        UnresolvedAttribute("a.age"),
        UnresolvedAttribute("a.state"),
        UnresolvedAttribute("a.country"),
        UnresolvedAttribute("b.occupation"),
        UnresolvedAttribute("b.country"),
        UnresolvedAttribute("b.salary")),
      joinPlan)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test join then stats") {
    val frame = sql(s"""
         | source = $testTable1
         | | inner join left=a, right=b
         |     ON a.name = b.name
         |     $testTable2
         | | stats avg(salary) by span(age, 10) as age_span
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] =
      Array(Row(100000.0, 70), Row(105000.0, 20), Row(60000.0, 40), Row(70000.0, 30))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val plan1 = SubqueryAlias("a", table1)
    val plan2 = SubqueryAlias("b", table2)

    val joinCondition = EqualTo(UnresolvedAttribute("a.name"), UnresolvedAttribute("b.name"))
    val joinPlan = Join(plan1, plan2, Inner, Some(joinCondition), JoinHint.NONE)
    val salaryField = UnresolvedAttribute("salary")
    val star = Seq(UnresolvedStar(None))
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(salaryField), isDistinct = false), "avg(salary)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan =
      Aggregate(Seq(span), Seq(aggregateExpressions, span), joinPlan)

    val expectedPlan = Project(star, aggregatePlan)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test join then stats with group by") {
    val frame = sql(s"""
         | source = $testTable1
         | | inner join left=a, right=b
         |     ON a.name = b.name
         |     $testTable2
         | | stats avg(salary) by span(age, 10) as age_span, b.country
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row(120000.0, "USA", 40),
      Row(0.0, "Canada", 40),
      Row(70000.0, "USA", 30),
      Row(100000.0, "England", 70),
      Row(105000.0, "Canada", 20))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val plan1 = SubqueryAlias("a", table1)
    val plan2 = SubqueryAlias("b", table2)

    val joinCondition = EqualTo(UnresolvedAttribute("a.name"), UnresolvedAttribute("b.name"))
    val joinPlan = Join(plan1, plan2, Inner, Some(joinCondition), JoinHint.NONE)

    val salaryField = UnresolvedAttribute("salary")
    val countryField = UnresolvedAttribute("b.country")
    val countryAlias = Alias(countryField, "b.country")()
    val star = Seq(UnresolvedStar(None))
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(salaryField), isDistinct = false), "avg(salary)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan =
      Aggregate(Seq(countryAlias, span), Seq(aggregateExpressions, countryAlias, span), joinPlan)

    val expectedPlan = Project(star, aggregatePlan)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test complex inner join") {
    val frame = sql(s"""
         | source = $testTable1
         | | where country = 'USA' OR country = 'England'
         | | inner join left=a, right=b
         |     ON a.name = b.name
         |     $testTable2
         | | stats avg(salary) by span(age, 10) as age_span, b.country
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row(120000.0, "USA", 40),
      Row(0.0, "Canada", 40),
      Row(70000.0, "USA", 30),
      Row(100000.0, "England", 70))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val filterExpr = Or(
      EqualTo(UnresolvedAttribute("country"), Literal("USA")),
      EqualTo(UnresolvedAttribute("country"), Literal("England")))
    val plan1 = SubqueryAlias("a", Filter(filterExpr, table1))
    val plan2 = SubqueryAlias("b", table2)

    val joinCondition = EqualTo(UnresolvedAttribute("a.name"), UnresolvedAttribute("b.name"))
    val joinPlan = Join(plan1, plan2, Inner, Some(joinCondition), JoinHint.NONE)

    val salaryField = UnresolvedAttribute("salary")
    val countryField = UnresolvedAttribute("b.country")
    val countryAlias = Alias(countryField, "b.country")()
    val star = Seq(UnresolvedStar(None))
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(salaryField), isDistinct = false), "avg(salary)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan =
      Aggregate(Seq(countryAlias, span), Seq(aggregateExpressions, countryAlias, span), joinPlan)

    val expectedPlan = Project(star, aggregatePlan)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test complex left outer join") {
    val frame = sql(s"""
         | source = $testTable1
         | | where country = 'Canada' OR country = 'England'
         | | left join left=a, right=b
         |     ON a.name = b.name
         |     $testTable2
         | | sort a.age
         | | fields a.name, a.age, a.state, a.country, b.occupation, b.country, b.salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("Rick", 70, "B.C", "Canada", null, null, null),
      Row("Jim", 27, "B.C", "Canada", null, null, null),
      Row("Peter", 57, "B.C", "Canada", null, null, null),
      Row("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000),
      Row("Jane", 20, "Quebec", "Canada", "Scientist", "Canada", 90000))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val filterExpr = Or(
      EqualTo(UnresolvedAttribute("country"), Literal("Canada")),
      EqualTo(UnresolvedAttribute("country"), Literal("England")))
    val plan1 = SubqueryAlias("a", Filter(filterExpr, table1))
    val plan2 = SubqueryAlias("b", table2)

    val joinCondition = EqualTo(UnresolvedAttribute("a.name"), UnresolvedAttribute("b.name"))
    val joinPlan = Join(plan1, plan2, LeftOuter, Some(joinCondition), JoinHint.NONE)
    val sort =
      Sort(Seq(SortOrder(UnresolvedAttribute("a.age"), Ascending)), global = true, joinPlan)
    val expectedPlan = Project(
      Seq(
        UnresolvedAttribute("a.name"),
        UnresolvedAttribute("a.age"),
        UnresolvedAttribute("a.state"),
        UnresolvedAttribute("a.country"),
        UnresolvedAttribute("b.occupation"),
        UnresolvedAttribute("b.country"),
        UnresolvedAttribute("b.salary")),
      sort)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test complex right outer join") {
    val frame = sql(s"""
         | source = $testTable1
         | | where country = 'Canada' OR country = 'England'
         | | right join left=a, right=b
         |     ON a.name = b.name
         |     $testTable2
         | | sort a.age
         | | fields a.name, a.age, a.state, a.country, b.occupation, b.country, b.salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row(null, null, null, null, "Doctor", "USA", 120000),
      Row(null, null, null, null, "Unemployed", "Canada", 0),
      Row(null, null, null, null, "Engineer", "England", 100000),
      Row(null, null, null, null, "Artist", "USA", 70000),
      Row("Jane", 20, "Quebec", "Canada", "Scientist", "Canada", 90000),
      Row("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](6))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val filterExpr = Or(
      EqualTo(UnresolvedAttribute("country"), Literal("Canada")),
      EqualTo(UnresolvedAttribute("country"), Literal("England")))
    val plan1 = SubqueryAlias("a", Filter(filterExpr, table1))
    val plan2 = SubqueryAlias("b", table2)

    val joinCondition = EqualTo(UnresolvedAttribute("a.name"), UnresolvedAttribute("b.name"))
    val joinPlan = Join(plan1, plan2, RightOuter, Some(joinCondition), JoinHint.NONE)
    val sort =
      Sort(Seq(SortOrder(UnresolvedAttribute("a.age"), Ascending)), global = true, joinPlan)
    val expectedPlan = Project(
      Seq(
        UnresolvedAttribute("a.name"),
        UnresolvedAttribute("a.age"),
        UnresolvedAttribute("a.state"),
        UnresolvedAttribute("a.country"),
        UnresolvedAttribute("b.occupation"),
        UnresolvedAttribute("b.country"),
        UnresolvedAttribute("b.salary")),
      sort)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test complex left semi join") {
    val frame = sql(s"""
         | source = $testTable1
         | | where country = 'Canada' OR country = 'England'
         | | left semi join left=a, right=b
         |     ON a.name = b.name
         |     $testTable2
         | | sort a.age
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("John", 25, "Ontario", "Canada", 2023, 4),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val filterExpr = Or(
      EqualTo(UnresolvedAttribute("country"), Literal("Canada")),
      EqualTo(UnresolvedAttribute("country"), Literal("England")))
    val plan1 = SubqueryAlias("a", Filter(filterExpr, table1))
    val plan2 = SubqueryAlias("b", table2)

    val joinCondition = EqualTo(UnresolvedAttribute("a.name"), UnresolvedAttribute("b.name"))
    val joinPlan = Join(plan1, plan2, LeftSemi, Some(joinCondition), JoinHint.NONE)
    val sort =
      Sort(Seq(SortOrder(UnresolvedAttribute("a.age"), Ascending)), global = true, joinPlan)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), sort)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test complex left anti join") {
    val frame = sql(s"""
         | source = $testTable1
         | | where country = 'Canada' OR country = 'England'
         | | left anti join left=a, right=b
         |     ON a.name = b.name
         |     $testTable2
         | | sort a.age
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("Jim", 27, "B.C", "Canada", 2023, 4),
      Row("Peter", 57, "B.C", "Canada", 2023, 4),
      Row("Rick", 70, "B.C", "Canada", 2023, 4))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val filterExpr = Or(
      EqualTo(UnresolvedAttribute("country"), Literal("Canada")),
      EqualTo(UnresolvedAttribute("country"), Literal("England")))
    val plan1 = SubqueryAlias("a", Filter(filterExpr, table1))
    val plan2 = SubqueryAlias("b", table2)

    val joinCondition = EqualTo(UnresolvedAttribute("a.name"), UnresolvedAttribute("b.name"))
    val joinPlan = Join(plan1, plan2, LeftAnti, Some(joinCondition), JoinHint.NONE)
    val sort =
      Sort(Seq(SortOrder(UnresolvedAttribute("a.age"), Ascending)), global = true, joinPlan)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), sort)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test cross join") {
    val frame = sql(s"""
         | source = $testTable1
         | | where country = 'Canada' OR country = 'England'
         | | join left=a, right=b
         |     $testTable2
         | | sort a.age
         | | fields a.name, a.age, a.state, a.country, b.occupation, b.country, b.salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("Jane", 20, "Quebec", "Canada", "Engineer", "England", 100000),
      Row("Jane", 20, "Quebec", "Canada", "Artist", "USA", 70000),
      Row("Jane", 20, "Quebec", "Canada", "Doctor", "Canada", 120000),
      Row("Jane", 20, "Quebec", "Canada", "Doctor", "USA", 120000),
      Row("Jane", 20, "Quebec", "Canada", "Unemployed", "Canada", 0),
      Row("Jane", 20, "Quebec", "Canada", "Scientist", "Canada", 90000),
      Row("John", 25, "Ontario", "Canada", "Engineer", "England", 100000),
      Row("John", 25, "Ontario", "Canada", "Artist", "USA", 70000),
      Row("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000),
      Row("John", 25, "Ontario", "Canada", "Doctor", "USA", 120000),
      Row("John", 25, "Ontario", "Canada", "Unemployed", "Canada", 0),
      Row("John", 25, "Ontario", "Canada", "Scientist", "Canada", 90000),
      Row("Jim", 27, "B.C", "Canada", "Engineer", "England", 100000),
      Row("Jim", 27, "B.C", "Canada", "Artist", "USA", 70000),
      Row("Jim", 27, "B.C", "Canada", "Doctor", "Canada", 120000),
      Row("Jim", 27, "B.C", "Canada", "Doctor", "USA", 120000),
      Row("Jim", 27, "B.C", "Canada", "Unemployed", "Canada", 0),
      Row("Jim", 27, "B.C", "Canada", "Scientist", "Canada", 90000),
      Row("Peter", 57, "B.C", "Canada", "Engineer", "England", 100000),
      Row("Peter", 57, "B.C", "Canada", "Artist", "USA", 70000),
      Row("Peter", 57, "B.C", "Canada", "Doctor", "Canada", 120000),
      Row("Peter", 57, "B.C", "Canada", "Doctor", "USA", 120000),
      Row("Peter", 57, "B.C", "Canada", "Unemployed", "Canada", 0),
      Row("Peter", 57, "B.C", "Canada", "Scientist", "Canada", 90000),
      Row("Rick", 70, "B.C", "Canada", "Engineer", "England", 100000),
      Row("Rick", 70, "B.C", "Canada", "Artist", "USA", 70000),
      Row("Rick", 70, "B.C", "Canada", "Doctor", "Canada", 120000),
      Row("Rick", 70, "B.C", "Canada", "Doctor", "USA", 120000),
      Row("Rick", 70, "B.C", "Canada", "Unemployed", "Canada", 0),
      Row("Rick", 70, "B.C", "Canada", "Scientist", "Canada", 90000))

    implicit val rowOrdering: Ordering[Row] = Ordering.by { row: Row =>
      (row.getAs[String](0), row.getAs[String](4), row.getAs[String](5))
    }
    assert(results.sorted.sameElements(expectedResults.sorted))

    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val filterExpr = Or(
      EqualTo(UnresolvedAttribute("country"), Literal("Canada")),
      EqualTo(UnresolvedAttribute("country"), Literal("England")))
    val plan1 = SubqueryAlias("a", Filter(filterExpr, table1))
    val plan2 = SubqueryAlias("b", table2)

    val joinPlan = Join(plan1, plan2, Cross, None, JoinHint.NONE)
    val sort =
      Sort(Seq(SortOrder(UnresolvedAttribute("a.age"), Ascending)), global = true, joinPlan)
    val expectedPlan = Project(
      Seq(
        UnresolvedAttribute("a.name"),
        UnresolvedAttribute("a.age"),
        UnresolvedAttribute("a.state"),
        UnresolvedAttribute("a.country"),
        UnresolvedAttribute("b.occupation"),
        UnresolvedAttribute("b.country"),
        UnresolvedAttribute("b.salary")),
      sort)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test cross join with join criteria fallback to inner join") {
    val cross = sql(s"""
         | source = $testTable1
         | | where country = 'Canada' OR country = 'England'
         | | cross join left=a, right=b
         |     ON a.name = b.name
         |     $testTable2
         | | sort a.age
         | | fields a.name, a.age, a.state, a.country, b.occupation, b.country, b.salary
         | """.stripMargin)
    val results: Array[Row] = cross.collect()
    // results.foreach(println(_))
    val inner = sql(s"""
         | source = $testTable1
         | | where country = 'Canada' OR country = 'England'
         | | inner join left=a, right=b
         |     ON a.name = b.name
         |     $testTable2
         | | sort a.age
         | | fields a.name, a.age, a.state, a.country, b.occupation, b.country, b.salary
         | """.stripMargin)
    val expected: Array[Row] = inner.collect()

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expected.sorted))

    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val filterExpr = Or(
      EqualTo(UnresolvedAttribute("country"), Literal("Canada")),
      EqualTo(UnresolvedAttribute("country"), Literal("England")))
    val plan1 = SubqueryAlias("a", Filter(filterExpr, table1))
    val plan2 = SubqueryAlias("b", table2)

    val joinCondition = EqualTo(UnresolvedAttribute("a.name"), UnresolvedAttribute("b.name"))
    val joinPlan = Join(plan1, plan2, Cross, Some(joinCondition), JoinHint.NONE)
    val sort =
      Sort(Seq(SortOrder(UnresolvedAttribute("a.age"), Ascending)), global = true, joinPlan)
    val expectedPlan = Project(
      Seq(
        UnresolvedAttribute("a.name"),
        UnresolvedAttribute("a.age"),
        UnresolvedAttribute("a.state"),
        UnresolvedAttribute("a.country"),
        UnresolvedAttribute("b.occupation"),
        UnresolvedAttribute("b.country"),
        UnresolvedAttribute("b.salary")),
      sort)
    val logicalPlan: LogicalPlan = cross.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test non-equi join") {
    val frame = sql(s"""
         | source = $testTable1
         | | where country = 'USA' OR country = 'England'
         | | inner join left=a, right=b
         |     ON age < salary
         |     $testTable2
         | | where occupation = 'Doctor' OR occupation = 'Engineer'
         | | fields a.name, age, state, a.country, occupation, b.country, salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, "California", "USA", "Doctor", "USA", 120000),
      Row("Hello", 30, "New York", "USA", "Doctor", "USA", 120000),
      Row("Jake", 70, "California", "USA", "Engineer", "England", 100000),
      Row("Jake", 70, "California", "USA", "Doctor", "Canada", 120000),
      Row("Hello", 30, "New York", "USA", "Engineer", "England", 100000),
      Row("Hello", 30, "New York", "USA", "Doctor", "Canada", 120000),
      Row("David", 40, "Washington", "USA", "Doctor", "USA", 120000),
      Row("David", 40, "Washington", "USA", "Engineer", "England", 100000),
      Row("David", 40, "Washington", "USA", "Doctor", "Canada", 120000))

    implicit val rowOrdering: Ordering[Row] = Ordering.by { row: Row =>
      (row.getAs[String](0), row.getAs[String](2), row.getAs[String](4), row.getAs[String](5))
    }
    assert(results.sorted.sameElements(expectedResults.sorted))

    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val filterExpr1 = Or(
      EqualTo(UnresolvedAttribute("country"), Literal("USA")),
      EqualTo(UnresolvedAttribute("country"), Literal("England")))
    val plan1 = SubqueryAlias("a", Filter(filterExpr1, table1))
    val plan2 = SubqueryAlias("b", table2)

    val joinCondition = LessThan(UnresolvedAttribute("age"), UnresolvedAttribute("salary"))
    val joinPlan = Join(plan1, plan2, Inner, Some(joinCondition), JoinHint.NONE)
    val filterExpr2 = Or(
      EqualTo(UnresolvedAttribute("occupation"), Literal("Doctor")),
      EqualTo(UnresolvedAttribute("occupation"), Literal("Engineer")))
    val expectedPlan = Project(
      Seq(
        UnresolvedAttribute("a.name"),
        UnresolvedAttribute("age"),
        UnresolvedAttribute("state"),
        UnresolvedAttribute("a.country"),
        UnresolvedAttribute("occupation"),
        UnresolvedAttribute("b.country"),
        UnresolvedAttribute("salary")),
      Filter(filterExpr2, joinPlan))
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test multiple joins") {
    val frame = sql(s"""
         | source = $testTable1
         | | where country = 'Canada' OR country = 'England'
         | | inner join left=a, right=b
         |     ON a.name = b.name AND a.year = 2023 AND a.month = 4 AND b.year = 2023 AND b.month = 4
         |     $testTable2
         | | eval a_name = a.name
         | | eval a_country = a.country
         | | eval b_country = b.country
         | | fields a_name, age, state, a_country, occupation, b_country, salary
         | | left join left=a, right=b
         |     ON a.a_name = b.name
         |     $testTable3
         | | eval aa_country = a.a_country
         | | eval ab_country = a.b_country
         | | eval bb_country = b.country
         | | fields a_name, age, state, aa_country, occupation, ab_country, salary, bb_country, hobby, language
         | | cross join left=a, right=b
         |     $testTable2
         | | eval new_country = a.aa_country
         | | eval new_salary = b.salary
         | | stats avg(new_salary) as avg_salary by span(age, 5) as age_span, state
         | | left semi join left=a, right=b
         |     ON a.state = b.state
         |     $testTable1
         | | eval new_avg_salary = floor(avg_salary)
         | | fields state, age_span, new_avg_salary
         | """.stripMargin)
    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(Row("Quebec", 20, 83333), Row("Ontario", 25, 83333))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // println(frame.queryExecution.optimizedPlan)
    assert(frame.queryExecution.optimizedPlan.collect {
      case j @ Join(_, _, Cross, None, JoinHint.NONE) => j
    }.size == 1)
    assert(frame.queryExecution.optimizedPlan.collect {
      case j @ Join(_, _, LeftOuter, _, JoinHint.NONE) => j
    }.size == 1)
    assert(frame.queryExecution.optimizedPlan.collect {
      case j @ Join(_, _, Inner, _, JoinHint.NONE) => j
    }.size == 1)
  }
}
