/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, Descending, Divide, EqualTo, Floor, GreaterThan, LessThan, LessThanOrEqual, Literal, Multiply, Not, Or, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Limit, LogicalPlan, Project, Sort}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    // Update table creation
    sql(s"""
         | CREATE TABLE $testTable
         | (
         |   name STRING,
         |   age INT,
         |   state STRING,
         |   country STRING
         | )
         | USING CSV
         | OPTIONS (
         |  header 'false',
         |  delimiter '\t'
         | )
         | PARTITIONED BY (
         |    year INT,
         |    month INT
         | )
         |""".stripMargin)

    // Update data insertion
    sql(s"""
         | INSERT INTO $testTable
         | PARTITION (year=2023, month=4)
         | VALUES ('Jake', 70, 'California', 'USA'),
         |        ('Hello', 30, 'New York', 'USA'),
         |        ('John', 25, 'Ontario', 'Canada'),
         |        ('Jane', 20, 'Quebec', 'Canada')
         | """.stripMargin)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("create ppl simple query test") {
    val frame = sql(s"""
         | source = $testTable
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, "California", "USA", 2023, 4),
      Row("Hello", 30, "New York", "USA", 2023, 4),
      Row("John", 25, "Ontario", "Canada", 2023, 4),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4))
    // Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val expectedPlan: LogicalPlan =
      Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("default", "flint_ppl_test")))
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("create ppl simple query with head (limit) 3 test") {
    val frame = sql(s"""
         | source = $testTable| head 2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 2)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Limit(
      Literal(2),
      Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("default", "flint_ppl_test"))))
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query with head (limit) and sorted test") {
    val frame = sql(s"""
         | source = $testTable| sort name | head 2 
         | """.stripMargin)

    // Retrieve the results
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 2)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Limit(
      Literal(2),
      Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("default", "flint_ppl_test"))))
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("name"), Ascending)), global = true, expectedPlan)
    // Compare the two plans
    assert(compareByString(sortedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query two with fields result test") {
    val frame = sql(s"""
         | source = $testTable| fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row("Jake", 70), Row("Hello", 30), Row("John", 25), Row("Jane", 20))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Project(
      Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age")),
      UnresolvedRelation(Seq("default", "flint_ppl_test")))
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("create ppl simple sorted query two with fields result test sorted") {
    val frame = sql(s"""
         | source = $testTable| sort age | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row("Jane", 20), Row("John", 25), Row("Hello", 30), Row("Jake", 70))
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Project(
      Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age")),
      UnresolvedRelation(Seq("default", "flint_ppl_test")))

    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("age"), Ascending)), global = true, expectedPlan)
    // Compare the two plans
    assert(sortedPlan === logicalPlan)
  }

  test("create ppl simple query two with fields and head (limit) test") {
    val frame = sql(s"""
         | source = $testTable| fields name, age | head 1
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val project = Project(
      Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age")),
      UnresolvedRelation(Seq("default", "flint_ppl_test")))
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Limit(Literal(1), Project(Seq(UnresolvedStar(None)), project))
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
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
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = EqualTo(UnresolvedAttribute("age"), Literal(25))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
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
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = And(
      Not(EqualTo(UnresolvedAttribute("country"), Literal("USA"))),
      GreaterThan(UnresolvedAttribute("age"), Literal(10)))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
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
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = And(
      Not(EqualTo(UnresolvedAttribute("country"), Literal("USA"))),
      GreaterThan(UnresolvedAttribute("age"), Literal(10)))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)

    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("age"), Descending)), global = true, expectedPlan)
    // Compare the two plans
    assert(sortedPlan === logicalPlan)
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
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = Or(
      EqualTo(UnresolvedAttribute("country"), Literal("USA")),
      LessThanOrEqual(UnresolvedAttribute("age"), Literal(20)))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
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
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = Or(
      EqualTo(UnresolvedAttribute("country"), Literal("USA")),
      LessThanOrEqual(UnresolvedAttribute("age"), Literal(20)))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val projectPlan = Project(Seq(UnresolvedStar(None)), Project(projectList, filterPlan))
    val expectedPlan = Limit(Literal(1), projectPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
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
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = GreaterThan(UnresolvedAttribute("age"), Literal(25))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
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
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = LessThanOrEqual(UnresolvedAttribute("age"), Literal(65))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
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
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = LessThanOrEqual(UnresolvedAttribute("age"), Literal(65))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("name"), Ascending)), global = true, expectedPlan)
    // Compare the two plans
    assert(sortedPlan === logicalPlan)
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
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = EqualTo(UnresolvedAttribute("name"), Literal("Jake"))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
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
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = Not(EqualTo(UnresolvedAttribute("name"), Literal("Jake")))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("create ppl simple age avg query test") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) 
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(36.25))

    // Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val aggregateExpressions =
      Seq(Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")())
    val aggregatePlan = Project(aggregateExpressions, table)

    // Compare the two plans
    assert(compareByString(aggregatePlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age avg query with filter test") {
    val frame = sql(s"""
         | source = $testTable| where age < 50 | stats avg(age) 
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(25))

    // Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val filterExpr = LessThan(ageField, Literal(50))
    val filterPlan = Filter(filterExpr, table)
    val aggregateExpressions =
      Seq(Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")())
    val aggregatePlan = Project(aggregateExpressions, filterPlan)

    // Compare the two plans
    assert(compareByString(aggregatePlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age avg group by country query test ") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) by country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(22.5, "Canada"), Row(50.0, "USA"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age avg group by country head (limit) query test ") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) by country | head 1
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val projectPlan = Project(Seq(UnresolvedStar(None)), aggregatePlan)
    val expectedPlan = Limit(Literal(1), projectPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age max group by country query test ") {
    val frame = sql(s"""
         | source = $testTable| stats max(age) by country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(70, "USA"), Row(25, "Canada"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("MAX"), Seq(ageField), isDistinct = false), "max(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age min group by country query test ") {
    val frame = sql(s"""
         | source = $testTable| stats min(age) by country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(30, "USA"), Row(20, "Canada"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("MIN"), Seq(ageField), isDistinct = false), "min(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age sum group by country query test ") {
    val frame = sql(s"""
         | source = $testTable| stats sum(age) by country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(100L, "USA"), Row(45L, "Canada"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Long](_.getAs[Long](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("SUM"), Seq(ageField), isDistinct = false), "sum(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age sum group by country order by age query test with sort ") {
    val frame = sql(s"""
         | source = $testTable| stats sum(age) by country | sort country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(45L, "Canada"), Row(100L, "USA"))

    // Compare the results
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("SUM"), Seq(ageField), isDistinct = false), "sum(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val expectedPlan = Project(star, aggregatePlan)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("country"), Ascending)), global = true, expectedPlan)
    // Compare the two plans
    assert(compareByString(sortedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age count group by country query test ") {
    val frame = sql(s"""
         | source = $testTable| stats count(age) by country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(2L, "Canada"), Row(2L, "USA"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](1))
    assert(
      results.sorted.sameElements(expectedResults.sorted),
      s"Expected: ${expectedResults.mkString(", ")}, but got: ${results.mkString(", ")}")

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("COUNT"), Seq(ageField), isDistinct = false), "count(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(
      compareByString(expectedPlan) === compareByString(logicalPlan),
      s"Expected plan: ${compareByString(expectedPlan)}, but got: ${compareByString(logicalPlan)}")
  }

  test("create ppl simple age avg group by country with state filter query test ") {
    val frame = sql(s"""
         | source = $testTable|  where state != 'Quebec' | stats avg(age) by country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(25.0, "Canada"), Row(50.0, "USA"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val stateField = UnresolvedAttribute("state")
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val productAlias = Alias(countryField, "country")()
    val filterExpr = Not(EqualTo(stateField, Literal("Quebec")))
    val filterPlan = Filter(filterExpr, table)

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  /**
   * | age_span | count_age |
   * |:---------|----------:|
   * | 20       |         2 |
   * | 30       |         1 |
   * | 70       |         1 |
   */
  test("create ppl simple count age by span of interval of 10 years query test ") {
    val frame = sql(s"""
         | source = $testTable| stats count(age) by span(age, 10) as age_span
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(1, 70L), Row(1, 30L), Row(2, 20L))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Long](_.getAs[Long](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("COUNT"), Seq(ageField), isDistinct = false), "count(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "span (age,10,NONE)")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  ignore("create ppl simple count age by span of interval of 10 years query order by age test ") {
    val frame = sql(s"""
         | source = $testTable| stats count(age) by span(age, 10) as age_span | sort age_span
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(1, 70L), Row(1, 30L), Row(2, 20L))

    // Compare the results
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("COUNT"), Seq(ageField), isDistinct = false), "count(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "span (age,10,NONE)")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), table)
    val expectedPlan = Project(star, aggregatePlan)
    val sortedPlan: LogicalPlan = Sort(
      Seq(SortOrder(UnresolvedAttribute("span (age,10,NONE)"), Ascending)),
      global = true,
      expectedPlan)
    // Compare the two plans
    assert(sortedPlan === logicalPlan)
  }

  /**
   * | age_span | average_age |
   * |:---------|------------:|
   * | 20       |        22.5 |
   * | 30       |          30 |
   * | 70       |          70 |
   */
  test("create ppl simple avg age by span of interval of 10 years query test ") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) by span(age, 10) as age_span
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(70d, 70L), Row(30d, 30L), Row(22.5d, 20L))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "span (age,10,NONE)")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple avg age by span of interval of 10 years with head (limit) query test ") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) by span(age, 10) as age_span | head 2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 2)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "span (age,10,NONE)")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), table)
    val projectPlan = Project(star, aggregatePlan)
    val expectedPlan = Limit(Literal(2), projectPlan)

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
  ignore("create ppl average age by span of interval of 10 years group by country query test ") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) by span(age, 10) as age_span, country
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(1, 70L), Row(1, 30L), Row(2, 20L))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Long](_.getAs[Long](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("COUNT"), Seq(ageField), isDistinct = false), "count(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "span (age,10,NONE)")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  ignore(
    "create ppl average age by span of interval of 10 years group by country head (limit) 2 query test ") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) by span(age, 10) as age_span, country | head 2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 2)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("COUNT"), Seq(ageField), isDistinct = false), "count(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "span (age,10,NONE)")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), table)
    val projectPlan = Project(star, aggregatePlan)
    val expectedPlan = Limit(Literal(1), projectPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  ignore(
    "create ppl average age by span of interval of 10 years group by country head (limit) 2 query and sort by test ") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) by span(age, 10) as age_span, country | head 2 | sort age_span
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 2)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("COUNT"), Seq(ageField), isDistinct = false), "count(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "span (age,10,NONE)")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), table)
    val projectPlan = Project(star, aggregatePlan)
    val expectedPlan = Limit(Literal(1), projectPlan)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("age"), Descending)), global = true, expectedPlan)
    // Compare the two plans
    assert(compareByString(sortedPlan) === compareByString(logicalPlan))
  }
}
