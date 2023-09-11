/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.FlintSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo, GreaterThan, LessThanOrEqual, Literal, Not}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project}
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.{QueryTest, Row}

class FlintSparkPPLITSuite
  extends QueryTest
    with LogicalPlanTestUtils
    with FlintSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    // Update table creation
    sql(
      s"""
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
    sql(
      s"""
         | INSERT INTO $testTable
         | PARTITION (year=2023, month=4)
         | VALUES ('Jake', 70, 'California', 'USA'),
         |        ('Hello', 30, 'New York', 'USA'),
         |        ('John', 25, 'Ontario', 'Canada'),
         |        ('Jane', 25, 'Quebec', 'Canada')
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

  test("create ppl simple query with start fields result test") {
    val frame = sql(
      s"""
         | source = $testTable
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
//    [John,25,Ontario,Canada,2023,4], [Jane,25,Quebec,Canada,2023,4], [Jake,70,California,USA,2023,4], [Hello,30,New York,USA,2023,4]
    val expectedResults: Array[Row] = Array(
      Row("Jake",70,"California","USA",2023,4),
      Row("Hello",30,"New York","USA",2023,4),
      Row("John",25,"Ontario","Canada",2023,4),
      Row("Jane",25,"Quebec","Canada",2023,4)
    )
    // Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Project(Seq(UnresolvedStar(None)), UnresolvedRelation(Seq("default", "flint_ppl_test")))
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("create ppl simple query two with fields result test") {
    val frame = sql(
      s"""
         | source = $testTable| fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70),
      Row("Hello", 30),
      Row("John", 25),
      Row("Jane", 25)
    )
    // Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Project(Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age")),
      UnresolvedRelation(Seq("default", "flint_ppl_test")))
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("create ppl simple age literal equal filter query with two fields result test") {
    val frame = sql(
      s"""
         | source = $testTable age=25 | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("John", 25),
      Row("Jane", 25)
    )
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

  test("create ppl simple age literal greater than filter query with two fields result test") {
    val frame = sql(
      s"""
         | source = $testTable age>25 | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70),
      Row("Hello", 30)
    )
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

  test("create ppl simple age literal smaller than equals filter query with two fields result test") {
    val frame = sql(
      s"""
         | source = $testTable age<=65 | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Hello", 30),
      Row("John", 25),
      Row("Jane", 25)
    )
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

  test("create ppl simple name literal equal filter query with two fields result test") {
    val frame = sql(
      s"""
         | source = $testTable name='Jake' | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70)
    )
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
    val frame = sql(
      s"""
         | source = $testTable name!='Jake' | fields name, age
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Hello", 30),
      Row("John", 25),
      Row("Jane", 25)
    )

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
    val frame = sql(
      s"""
         | source = $testTable| stats avg(age) 
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row(37.5),
    )

    // Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val priceField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_test"))
    val aggregateExpressions = Seq(Alias(UnresolvedFunction(Seq("AVG"), Seq(priceField), isDistinct = false), "avg(age)")())
    val aggregatePlan = Project(aggregateExpressions, table)

    // Compare the two plans
    assert(compareByString(aggregatePlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age avg group by country query test ") {
    val checkData = sql(s"SELECT country, AVG(age) AS avg_age FROM $testTable group by country");
    checkData.show()
    checkData.queryExecution.logical.show()

    val frame = sql(
      s"""
         | source = $testTable| stats avg(age) by country
         | """.stripMargin)


    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row(25.0,"Canada"),
      Row(50.0,"USA"),
    )

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
    val aggregateExpressions = Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan = Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }
}
