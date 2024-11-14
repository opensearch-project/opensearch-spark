/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, CaseWhen, Descending, Divide, EqualTo, Floor, GreaterThan, In, LessThan, LessThanOrEqual, Literal, Multiply, Not, Or, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLFiltersITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"
  private val duplicationTable = "spark_catalog.default.flint_ppl_test_duplication_table"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create test table
    createPartitionedStateCountryTable(testTable)
    createDuplicationNullableTable(duplicationTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("create ppl simple age literal equal filter query with two fields result test") {
    val frame = sql(s"""
         | source = $testTable age=25 | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("John", 25))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(UnresolvedAttribute("age"), Literal(25))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple age literal greater than filter AND country not equal filter query with two fields result test") {
    val frame = sql(s"""
         | source = $testTable age>10 and country != 'USA' | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("John", 25), Row("Jane", 20))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = And(
      GreaterThan(UnresolvedAttribute("age"), Literal(10)),
      Not(EqualTo(UnresolvedAttribute("country"), Literal("USA"))))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple age literal greater than filter AND country not equal filter query with two fields sorted result test") {
    val frame = sql(s"""
         | source = $testTable age>10 and country != 'USA' | sort - age | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("John", 25), Row("Jane", 20))
    // Compare the results
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = And(
      GreaterThan(UnresolvedAttribute("age"), Literal(10)),
      Not(EqualTo(UnresolvedAttribute("country"), Literal("USA"))))
    val filterPlan = Filter(filterExpr, table)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("age"), Descending)), global = true, filterPlan)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age")), sortedPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple age literal equal than filter OR country not equal filter query with two fields result test") {
    val frame = sql(s"""
         | source = $testTable age<=20 OR country = 'USA' | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Jane", 20), Row("Jake", 70), Row("Hello", 30))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = Or(
      LessThanOrEqual(UnresolvedAttribute("age"), Literal(20)),
      EqualTo(UnresolvedAttribute("country"), Literal("USA")))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple age literal equal than filter OR country not equal filter query with two fields result and head (limit) test") {
    val frame = sql(s"""
         | source = $testTable age<=20 OR country = 'USA' | fields name, age | head 1
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = Or(
      LessThanOrEqual(UnresolvedAttribute("age"), Literal(20)),
      EqualTo(UnresolvedAttribute("country"), Literal("USA")))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val limitPlan = Limit(Literal(1), Project(projectList, filterPlan))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), limitPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age literal greater than filter query with two fields result test") {
    val frame = sql(s"""
         | source = $testTable age>25 | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Jake", 70), Row("Hello", 30))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = GreaterThan(UnresolvedAttribute("age"), Literal(25))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple age literal smaller than equals filter query with two fields result test") {
    val frame = sql(s"""
         | source = $testTable age<=65 | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Hello", 30), Row("John", 25), Row("Jane", 20))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = LessThanOrEqual(UnresolvedAttribute("age"), Literal(65))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple age literal smaller than equals filter query with two fields result with sort test") {
    val frame = sql(s"""
         | source = $testTable age<=65 | sort name | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Hello", 30), Row("Jane", 20), Row("John", 25))
    // Compare the results
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = LessThanOrEqual(UnresolvedAttribute("age"), Literal(65))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("name"), Ascending)), global = true, filterPlan)
    val expectedPlan = Project(projectList, sortedPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple name literal equal filter query with two fields result test") {
    val frame = sql(s"""
         | source = $testTable name='Jake' | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Jake", 70))
    //     Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(UnresolvedAttribute("name"), Literal("Jake"))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }
  test("create ppl simple name literal not equal filter query with two fields result test") {
    val frame = sql(s"""
         | source = $testTable name!='Jake' | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Hello", 30), Row("John", 25), Row("Jane", 20))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = Not(EqualTo(UnresolvedAttribute("name"), Literal("Jake")))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  /**
   * | age_span | country | average_age |
   * |:---------|:--------|:------------|
   * | 20       | Canada  | 22.5        |
   * | 30       | USA     | 30          |
   * | 70       | USA     | 70          |
   */
  test("create ppl average age by span of interval of 10 years group by country query test ") {
    val dataFrame = spark.sql(
      "SELECT FLOOR(age / 10) * 10 AS age_span, country, AVG(age) AS average_age FROM default.flint_ppl_test GROUP BY FLOOR(age / 10) * 10, country ")
    dataFrame.collect();
    dataFrame.show()

    val frame = sql(s"""
         | source = $testTable| stats avg(age) by span(age, 10) as age_span, country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row(70.0d, "USA", 70L), Row(30.0d, "USA", 30L), Row(22.5d, "Canada", 20L))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val countryField = UnresolvedAttribute("country")
    val countryAlias = Alias(countryField, "country")()

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan =
      Aggregate(Seq(countryAlias, span), Seq(aggregateExpressions, countryAlias, span), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("case function used as filter") {
    val frame = sql(s"""
                       |  source = $testTable case(country = 'USA', 'The United States of America' else 'Other country') = 'The United States of America'
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, "California", "USA", 2023, 4),
      Row("Hello", 30, "New York", "USA", 2023, 4))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))

    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val conditionValueSequence = Seq(
      (
        EqualTo(Literal(true), EqualTo(UnresolvedAttribute("country"), Literal("USA"))),
        Literal("The United States of America")))
    val elseValue = Literal("Other country")
    val caseFunction = CaseWhen(conditionValueSequence, elseValue)
    val filterExpr = EqualTo(caseFunction, Literal("The United States of America"))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("case function used as filter complex filter") {
    val frame = sql(s"""
                       |  source = $duplicationTable
                       |  | eval factor = case(id > 15, id - 14, isnull(name), id - 7, id < 3, id + 1 else 1)
                       |  | where case(factor = 2, 'even', factor = 4, 'even', factor = 6, 'even', factor = 8, 'even' else 'odd') = 'even'
                       |  |  stats count() by factor
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect() // count(), factor
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(1, 4), Row(1, 6), Row(2, 2))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table =
      UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_duplication_table"))

    // case function used in eval command
    val conditionValueEval = Seq(
      (
        EqualTo(Literal(true), GreaterThan(UnresolvedAttribute("id"), Literal(15))),
        UnresolvedFunction("-", Seq(UnresolvedAttribute("id"), Literal(14)), isDistinct = false)),
      (
        EqualTo(
          Literal(true),
          UnresolvedFunction("isnull", Seq(UnresolvedAttribute("name")), isDistinct = false)),
        UnresolvedFunction("-", Seq(UnresolvedAttribute("id"), Literal(7)), isDistinct = false)),
      (
        EqualTo(Literal(true), LessThan(UnresolvedAttribute("id"), Literal(3))),
        UnresolvedFunction("+", Seq(UnresolvedAttribute("id"), Literal(1)), isDistinct = false)))
    val aliasCaseFactor = Alias(CaseWhen(conditionValueEval, Literal(1)), "factor")()
    val evalProject = Project(Seq(UnresolvedStar(None), aliasCaseFactor), table)

    // case in where clause
    val conditionValueWhere = Seq(
      (
        EqualTo(Literal(true), EqualTo(UnresolvedAttribute("factor"), Literal(2))),
        Literal("even")),
      (
        EqualTo(Literal(true), EqualTo(UnresolvedAttribute("factor"), Literal(4))),
        Literal("even")),
      (
        EqualTo(Literal(true), EqualTo(UnresolvedAttribute("factor"), Literal(6))),
        Literal("even")),
      (
        EqualTo(Literal(true), EqualTo(UnresolvedAttribute("factor"), Literal(8))),
        Literal("even")))
    val caseFunctionWhere = CaseWhen(conditionValueWhere, Literal("odd"))
    val filterPlan = Filter(EqualTo(caseFunctionWhere, Literal("even")), evalProject)

    val aggregation = Aggregate(
      Seq(Alias(UnresolvedAttribute("factor"), "factor")()),
      Seq(
        Alias(
          UnresolvedFunction("COUNT", Seq(UnresolvedStar(None)), isDistinct = false),
          "count()")(),
        Alias(UnresolvedAttribute("factor"), "factor")()),
      filterPlan)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregation)
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test NOT IN expr in filter") {
    val frame = sql(s"""
                       | source = $testTable | where state not in ('California', 'New York') | fields state
                       | """.stripMargin)
    assertSameRows(Seq(Row("Ontario"), Row("Quebec")), frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val in = In(UnresolvedAttribute("state"), Seq(Literal("California"), Literal("New York")))
    val filter = Filter(Not(in), table)
    val expectedPlan = Project(Seq(UnresolvedAttribute("state")), filter)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test parenthesis in filter") {
    val frame = sql(s"""
                       | source = $testTable | where country = 'Canada' or age > 60 and age < 25 | fields name, age, country
                       | """.stripMargin)
    assertSameRows(Seq(Row("John", 25, "Canada"), Row("Jane", 20, "Canada")), frame)

    val frameWithParenthesis = sql(s"""
                       | source = $testTable | where (country = 'Canada' or age > 60) and age < 25 | fields name, age, country
                       | """.stripMargin)
    assertSameRows(Seq(Row("Jane", 20, "Canada")), frameWithParenthesis)

    val logicalPlan: LogicalPlan = frameWithParenthesis.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filter = Filter(
      And(
        Or(
          EqualTo(UnresolvedAttribute("country"), Literal("Canada")),
          GreaterThan(UnresolvedAttribute("age"), Literal(60))),
        LessThan(UnresolvedAttribute("age"), Literal(25))),
      table)
    val expectedPlan = Project(
      Seq(
        UnresolvedAttribute("name"),
        UnresolvedAttribute("age"),
        UnresolvedAttribute("country")),
      filter)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test complex and nested parenthesis in filter") {
    val frame1 = sql(s"""
                        | source = $testTable | WHERE (age > 18 AND (state = 'California' OR state = 'New York'))
                        | """.stripMargin)
    assertSameRows(
      Seq(
        Row("Hello", 30, "New York", "USA", 2023, 4),
        Row("Jake", 70, "California", "USA", 2023, 4)),
      frame1)

    val frame2 = sql(s"""
                        | source = $testTable | WHERE ((((age > 18) AND ((((state = 'California') OR state = 'New York'))))))
                        | """.stripMargin)
    assertSameRows(
      Seq(
        Row("Hello", 30, "New York", "USA", 2023, 4),
        Row("Jake", 70, "California", "USA", 2023, 4)),
      frame2)

    val frame3 = sql(s"""
                        | source = $testTable | WHERE (year = 2023 AND (month BETWEEN 1 AND 6)) AND (age >= 31 OR country = 'Canada')
                        | """.stripMargin)
    assertSameRows(
      Seq(
        Row("John", 25, "Ontario", "Canada", 2023, 4),
        Row("Jake", 70, "California", "USA", 2023, 4),
        Row("Jane", 20, "Quebec", "Canada", 2023, 4)),
      frame3)

    val frame4 = sql(s"""
                        | source = $testTable | WHERE ((state = 'Texas' OR state = 'California') AND (age < 30 OR (country = 'USA' AND year > 2020)))
                        | """.stripMargin)
    assertSameRows(Seq(Row("Jake", 70, "California", "USA", 2023, 4)), frame4)

    val frame5 = sql(s"""
                        | source = $testTable | WHERE (LIKE(LOWER(name), 'a%') OR LIKE(LOWER(name), 'j%')) AND (LENGTH(state) > 6 OR (country = 'USA' AND age > 18))
                        | """.stripMargin)
    assertSameRows(
      Seq(
        Row("John", 25, "Ontario", "Canada", 2023, 4),
        Row("Jake", 70, "California", "USA", 2023, 4)),
      frame5)

    val frame6 = sql(s"""
                        | source = $testTable | WHERE (age BETWEEN 25 AND 40) AND ((state IN ('California', 'New York', 'Texas') AND year = 2023) OR (country != 'USA' AND (month = 1 OR month = 12)))
                        | """.stripMargin)
    assertSameRows(Seq(Row("Hello", 30, "New York", "USA", 2023, 4)), frame6)

    val frame7 = sql(s"""
                        | source = $testTable | WHERE NOT (age < 18 OR (state = 'Alaska' AND year < 2020)) AND (country = 'USA' OR (country = 'Mexico' AND month BETWEEN 6 AND 8))
                        | """.stripMargin)
    assertSameRows(
      Seq(
        Row("Jake", 70, "California", "USA", 2023, 4),
        Row("Hello", 30, "New York", "USA", 2023, 4)),
      frame7)

    val frame8 = sql(s"""
                        | source = $testTable | WHERE (NOT (year < 2020 OR age < 18)) AND ((state = 'Texas' AND month % 2 = 0) OR (country = 'Mexico' AND (year = 2023 OR (year = 2022 AND month > 6))))
                        | """.stripMargin)
    assertSameRows(Seq(), frame8)
  }
}
