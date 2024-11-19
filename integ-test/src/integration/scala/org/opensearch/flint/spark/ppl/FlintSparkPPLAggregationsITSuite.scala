/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, EqualTo, GreaterThan, LessThan, Literal, Not, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLAggregationsITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createPartitionedStateCountryTable(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
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
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val aggregateExpressions =
      Seq(Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")())
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
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
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = LessThan(ageField, Literal(50))
    val filterPlan = Filter(filterExpr, table)
    val aggregateExpressions =
      Seq(Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")())
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val countryAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, countryAlias), table)
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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val projectPlan = Limit(Literal(1), aggregatePlan)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), projectPlan)

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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("SUM"), Seq(ageField), isDistinct = false), "sum(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("country"), Ascending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

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

  test("create ppl age sample stddev") {
    val frame = sql(s"""
                       | source = $testTable| stats stddev_samp(age)
                       | """.stripMargin)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(22.86737122335374d))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](1))
    assert(
      results.sorted.sameElements(expectedResults.sorted),
      s"Expected: ${expectedResults.mkString(", ")}, but got: ${results.mkString(", ")}")
    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("STDDEV_SAMP"), Seq(ageField), isDistinct = false),
        "stddev_samp(age)")()
    val aggregatePlan =
      Aggregate(Seq.empty, Seq(aggregateExpressions), table)
    val expectedPlan = Project(star, aggregatePlan)
    // Compare the two plans
    assert(
      compareByString(expectedPlan) === compareByString(logicalPlan),
      s"Expected plan: ${compareByString(expectedPlan)}, but got: ${compareByString(logicalPlan)}")
  }

  test("create ppl age sample stddev group by country query test with sort") {
    val frame = sql(s"""
                       | source = $testTable | stats stddev_samp(age) by country | sort country
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row(3.5355339059327378d, "Canada"), Row(28.284271247461902d, "USA"))

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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("STDDEV_SAMP"), Seq(ageField), isDistinct = false),
        "stddev_samp(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("country"), Ascending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    // Compare the two plans
    assert(
      compareByString(expectedPlan) === compareByString(logicalPlan),
      s"Expected plan: ${compareByString(expectedPlan)}, but got: ${compareByString(logicalPlan)}")
  }

  test("create ppl age sample stddev group by country with state filter query test") {
    val frame = sql(s"""
                       | source = $testTable | where state != 'Ontario' | stats stddev_samp(age) by country
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(null, "Canada"), Row(28.284271247461902d, "USA"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val stateField = UnresolvedAttribute("state")
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val filterExpr = Not(EqualTo(stateField, Literal("Ontario")))
    val filterPlan = Filter(filterExpr, table)
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("STDDEV_SAMP"), Seq(ageField), isDistinct = false),
        "stddev_samp(age)")()
    val productAlias = Alias(countryField, "country")()
    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl age population stddev") {
    val frame = sql(s"""
                       | source = $testTable| stats stddev_pop(age)
                       | """.stripMargin)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(19.803724397193573d))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](1))
    assert(
      results.sorted.sameElements(expectedResults.sorted),
      s"Expected: ${expectedResults.mkString(", ")}, but got: ${results.mkString(", ")}")
    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("STDDEV_POP"), Seq(ageField), isDistinct = false),
        "stddev_pop(age)")()
    val aggregatePlan =
      Aggregate(Seq.empty, Seq(aggregateExpressions), table)
    val expectedPlan = Project(star, aggregatePlan)
    // Compare the two plans
    assert(
      compareByString(expectedPlan) === compareByString(logicalPlan),
      s"Expected plan: ${compareByString(expectedPlan)}, but got: ${compareByString(logicalPlan)}")
  }

  test("create ppl age population stddev group by country query test with sort") {
    val frame = sql(s"""
                       | source = $testTable | stats stddev_pop(age) by country | sort country
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(2.5d, "Canada"), Row(20d, "USA"))

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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("STDDEV_POP"), Seq(ageField), isDistinct = false),
        "stddev_pop(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("country"), Ascending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    // Compare the two plans
    assert(
      compareByString(expectedPlan) === compareByString(logicalPlan),
      s"Expected plan: ${compareByString(expectedPlan)}, but got: ${compareByString(logicalPlan)}")
  }

  test("create ppl age population stddev group by country with state filter query test") {
    val frame = sql(s"""
                       | source = $testTable | where state != 'Ontario' | stats stddev_pop(age) by country
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(0d, "Canada"), Row(20d, "USA"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val stateField = UnresolvedAttribute("state")
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val filterExpr = Not(EqualTo(stateField, Literal("Ontario")))
    val filterPlan = Filter(filterExpr, table)
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("STDDEV_POP"), Seq(ageField), isDistinct = false),
        "stddev_pop(age)")()
    val productAlias = Alias(countryField, "country")()
    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple age 50th percentile ") {
    val frame = sql(s"""
                       | source = $testTable| stats percentile(age, 50)
                       | """.stripMargin)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(27.5))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](1))
    assert(
      results.sorted.sameElements(expectedResults.sorted),
      s"Expected: ${expectedResults.mkString(", ")}, but got: ${results.mkString(", ")}")
    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val percentage = Literal("0.5")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("PERCENTILE"), Seq(ageField, percentage), isDistinct = false),
        "percentile(age, 50)")()
    val aggregatePlan =
      Aggregate(Seq.empty, Seq(aggregateExpressions), table)
    val expectedPlan = Project(star, aggregatePlan)
    // Compare the two plans
    assert(
      compareByString(expectedPlan) === compareByString(logicalPlan),
      s"Expected plan: ${compareByString(expectedPlan)}, but got: ${compareByString(logicalPlan)}")
  }

  test("create ppl simple age 20th percentile group by country query test with sort") {
    val frame = sql(s"""
                       | source = $testTable | stats percentile(age, 20) by country | sort country
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(21d, "Canada"), Row(38d, "USA"))

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
    val percentage = Literal("0.2")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("PERCENTILE"), Seq(ageField, percentage), isDistinct = false),
        "percentile(age, 20)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("country"), Ascending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    // Compare the two plans
    assert(
      compareByString(expectedPlan) === compareByString(logicalPlan),
      s"Expected plan: ${compareByString(expectedPlan)}, but got: ${compareByString(logicalPlan)}")
  }

  test("create ppl simple age 40th percentile group by country with state filter query test") {
    val frame = sql(s"""
                       | source = $testTable | where state != 'Ontario' | stats percentile(age, 40) by country
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(20d, "Canada"), Row(46d, "USA"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val stateField = UnresolvedAttribute("state")
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val percentage = Literal("0.4")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val filterExpr = Not(EqualTo(stateField, Literal("Ontario")))
    val filterPlan = Filter(filterExpr, table)
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("PERCENTILE"), Seq(ageField, percentage), isDistinct = false),
        "percentile(age, 40)")()
    val productAlias = Alias(countryField, "country")()
    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple age 40th percentile approx group by country with state filter query test") {
    val frame = sql(s"""
                       | source = $testTable | where state != 'Ontario' | stats percentile_approx(age, 40) by country
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(20d, "Canada"), Row(30d, "USA"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val stateField = UnresolvedAttribute("state")
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val percentage = Literal("0.4")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val filterExpr = Not(EqualTo(stateField, Literal("Ontario")))
    val filterPlan = Filter(filterExpr, table)
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(
          Seq("PERCENTILE_APPROX"),
          Seq(ageField, percentage),
          isDistinct = false),
        "percentile_approx(age, 40)")()
    val productAlias = Alias(countryField, "country")()
    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create failing ppl percentile approx - due to too high percentage value test") {
    val thrown = intercept[IllegalStateException] {
      val frame = sql(s"""
                         | source = $testTable | stats percentile_approx(age, 200) by country
                         | """.stripMargin)
    }
    assert(thrown.getMessage === "Unsupported value 'percent': 200 (expected: >= 0 <= 100))")
  }

  test("create failing ppl percentile approx - due to too low percentage value test") {
    val thrown = intercept[IllegalStateException] {
      val frame = sql(s"""
                         | source = $testTable | stats percentile_approx(age, -4) by country
                         | """.stripMargin)
    }
    assert(thrown.getMessage === "Unsupported value 'percent': -4 (expected: >= 0 <= 100))")
  }

  test("create ppl simple country distinct_count ") {
    val frame = sql(s"""
                       | source = $testTable| stats distinct_count(country)
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()

    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(2L))

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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(countryField), isDistinct = true),
        "distinct_count(country)")()

    val aggregatePlan =
      Aggregate(Seq.empty, Seq(aggregateExpressions), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    comparePlans(expectedPlan, logicalPlan, false)
  }

  test("create ppl simple country distinct_count using approximation ") {
    val frame = sql(s"""
                       | source = $testTable| stats distinct_count_approx(country)
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()

    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(2L))

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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("APPROX_COUNT_DISTINCT"), Seq(countryField), isDistinct = true),
        "distinct_count_approx(country)")()

    val aggregatePlan =
      Aggregate(Seq.empty, Seq(aggregateExpressions), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    comparePlans(expectedPlan, logicalPlan, false)
  }

  test("create ppl simple age distinct_count group by country query test with sort") {
    val frame = sql(s"""
                       | source = $testTable | stats distinct_count(age) by country | sort country
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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(ageField), isDistinct = true),
        "distinct_count(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("country"), Ascending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    // Compare the two plans
    assert(
      compareByString(expectedPlan) === compareByString(logicalPlan),
      s"Expected plan: ${compareByString(expectedPlan)}, but got: ${compareByString(logicalPlan)}")
  }

  test(
    "create ppl simple age distinct_count group by country query test with sort using approximation") {
    val frame = sql(s"""
                       | source = $testTable | stats distinct_count_approx(age) by country | sort country
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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("APPROX_COUNT_DISTINCT"), Seq(ageField), isDistinct = true),
        "distinct_count_approx(age)")()
    val productAlias = Alias(countryField, "country")()

    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), table)
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("country"), Ascending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)

    // Compare the two plans
    assert(
      compareByString(expectedPlan) === compareByString(logicalPlan),
      s"Expected plan: ${compareByString(expectedPlan)}, but got: ${compareByString(logicalPlan)}")
  }

  test("create ppl simple age distinct_count group by country with state filter query test") {
    val frame = sql(s"""
                       | source = $testTable | where state != 'Ontario' | stats distinct_count(age) by country
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(1L, "Canada"), Row(2L, "USA"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val stateField = UnresolvedAttribute("state")
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val filterExpr = Not(EqualTo(stateField, Literal("Ontario")))
    val filterPlan = Filter(filterExpr, table)
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(ageField), isDistinct = true),
        "distinct_count(age)")()
    val productAlias = Alias(countryField, "country")()
    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl simple age distinct_count group by country with state filter query test using approximation") {
    val frame = sql(s"""
                       | source = $testTable | where state != 'Ontario' | stats distinct_count_approx(age) by country
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(1L, "Canada"), Row(2L, "USA"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val stateField = UnresolvedAttribute("state")
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes = Seq(Alias(countryField, "country")())
    val filterExpr = Not(EqualTo(stateField, Literal("Ontario")))
    val filterPlan = Filter(filterExpr, table)
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("APPROX_COUNT_DISTINCT"), Seq(ageField), isDistinct = true),
        "distinct_count_approx(age)")()
    val productAlias = Alias(countryField, "country")()
    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, productAlias), filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("two-level stats") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) as avg_age by state, country | stats avg(avg_age) as avg_state_age by country
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(22.5, "Canada"), Row(50.0, "USA"))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val star = Seq(UnresolvedStar(None))
    val stateField = UnresolvedAttribute("state")
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val avgAgeField = UnresolvedAttribute("avg_age")
    val stateAlias = Alias(stateField, "state")()
    val countryAlias = Alias(countryField, "country")()
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes1 = Seq(stateAlias, countryAlias)
    val aggregateExpressions1 =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg_age")()
    val aggregatePlan1 =
      Aggregate(groupByAttributes1, Seq(aggregateExpressions1, stateAlias, countryAlias), table)

    val groupByAttributes2 = Seq(countryAlias)
    val aggregateExpressions2 =
      Alias(
        UnresolvedFunction(Seq("AVG"), Seq(avgAgeField), isDistinct = false),
        "avg_state_age")()

    val aggregatePlan2 =
      Aggregate(groupByAttributes2, Seq(aggregateExpressions2, countryAlias), aggregatePlan1)
    val expectedPlan = Project(star, aggregatePlan2)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("two-level stats with eval") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) as avg_age by state, country | eval new_avg_age = avg_age - 10 | stats avg(new_avg_age) as avg_state_age by country
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(12.5, "Canada"), Row(40.0, "USA"))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val star = Seq(UnresolvedStar(None))
    val stateField = UnresolvedAttribute("state")
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val avgAgeField = UnresolvedAttribute("avg_age")
    val stateAlias = Alias(stateField, "state")()
    val countryAlias = Alias(countryField, "country")()
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes1 = Seq(stateAlias, countryAlias)
    val aggregateExpressions1 =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg_age")()
    val aggregatePlan1 =
      Aggregate(groupByAttributes1, Seq(aggregateExpressions1, stateAlias, countryAlias), table)

    val newAvgAgeAlias =
      Alias(
        UnresolvedFunction(Seq("-"), Seq(avgAgeField, Literal(10)), isDistinct = false),
        "new_avg_age")()
    val evalProject = Project(Seq(UnresolvedStar(None), newAvgAgeAlias), aggregatePlan1)

    val groupByAttributes2 = Seq(countryAlias)
    val aggregateExpressions2 =
      Alias(
        UnresolvedFunction(
          Seq("AVG"),
          Seq(UnresolvedAttribute("new_avg_age")),
          isDistinct = false),
        "avg_state_age")()

    val aggregatePlan2 =
      Aggregate(groupByAttributes2, Seq(aggregateExpressions2, countryAlias), evalProject)
    val expectedPlan = Project(star, aggregatePlan2)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("two-level stats with filter") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) as avg_age by country, state | where avg_age > 0 | stats count(avg_age) as count_state_age by country
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(2L, "Canada"), Row(2L, "USA"))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val star = Seq(UnresolvedStar(None))
    val stateField = UnresolvedAttribute("state")
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val avgAgeField = UnresolvedAttribute("avg_age")
    val stateAlias = Alias(stateField, "state")()
    val countryAlias = Alias(countryField, "country")()
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes1 = Seq(countryAlias, stateAlias)
    val aggregateExpressions1 =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg_age")()
    val aggregatePlan1 =
      Aggregate(groupByAttributes1, Seq(aggregateExpressions1, countryAlias, stateAlias), table)

    val filterExpr = GreaterThan(avgAgeField, Literal(0))
    val filterPlan = Filter(filterExpr, aggregatePlan1)

    val groupByAttributes2 = Seq(countryAlias)
    val aggregateExpressions2 =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(avgAgeField), isDistinct = false),
        "count_state_age")()

    val aggregatePlan2 =
      Aggregate(groupByAttributes2, Seq(aggregateExpressions2, countryAlias), filterPlan)
    val expectedPlan = Project(star, aggregatePlan2)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("three-level stats with eval and filter") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) as avg_age by country, state, name | eval avg_age_divide_20 = avg_age - 20 | stats avg(avg_age_divide_20)
         | as avg_state_age by country, state | where avg_state_age > 0 | stats count(avg_state_age) as count_country_age_greater_20 by country
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(1L, "Canada"), Row(2L, "USA"))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val star = Seq(UnresolvedStar(None))
    val nameField = UnresolvedAttribute("name")
    val stateField = UnresolvedAttribute("state")
    val countryField = UnresolvedAttribute("country")
    val ageField = UnresolvedAttribute("age")
    val avgAgeField = UnresolvedAttribute("avg_age")
    val nameAlias = Alias(nameField, "name")()
    val stateAlias = Alias(stateField, "state")()
    val countryAlias = Alias(countryField, "country")()
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val groupByAttributes1 = Seq(countryAlias, stateAlias, nameAlias)
    val aggregateExpressions1 =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg_age")()
    val aggregatePlan1 =
      Aggregate(
        groupByAttributes1,
        Seq(aggregateExpressions1, countryAlias, stateAlias, nameAlias),
        table)

    val avg_age_divide_20_Alias =
      Alias(
        UnresolvedFunction(Seq("-"), Seq(avgAgeField, Literal(20)), isDistinct = false),
        "avg_age_divide_20")()
    val evalProject = Project(Seq(UnresolvedStar(None), avg_age_divide_20_Alias), aggregatePlan1)
    val groupByAttributes2 = Seq(countryAlias, stateAlias)
    val aggregateExpressions2 =
      Alias(
        UnresolvedFunction(
          Seq("AVG"),
          Seq(UnresolvedAttribute("avg_age_divide_20")),
          isDistinct = false),
        "avg_state_age")()
    val aggregatePlan2 =
      Aggregate(
        groupByAttributes2,
        Seq(aggregateExpressions2, countryAlias, stateAlias),
        evalProject)

    val filterExpr = GreaterThan(UnresolvedAttribute("avg_state_age"), Literal(0))
    val filterPlan = Filter(filterExpr, aggregatePlan2)

    val groupByAttributes3 = Seq(countryAlias)
    val aggregateExpressions3 =
      Alias(
        UnresolvedFunction(
          Seq("COUNT"),
          Seq(UnresolvedAttribute("avg_state_age")),
          isDistinct = false),
        "count_country_age_greater_20")()

    val aggregatePlan3 =
      Aggregate(groupByAttributes3, Seq(aggregateExpressions3, countryAlias), filterPlan)
    val expectedPlan = Project(star, aggregatePlan3)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test count() at the first of stats clause") {
    val frame = sql(s"""
                       | source = $testTable | eval a = 1 | stats count() as cnt, sum(a) as sum, avg(a) as avg
                       | """.stripMargin)
    assertSameRows(Seq(Row(4, 4, 1.0)), frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val eval = Project(Seq(UnresolvedStar(None), Alias(Literal(1), "a")()), table)
    val sum =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "sum")()
    val avg =
      Alias(
        UnresolvedFunction(Seq("AVG"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "avg")()
    val count =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
        "cnt")()
    val aggregate = Aggregate(Seq.empty, Seq(count, sum, avg), eval)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregate)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test count() in the middle of stats clause") {
    val frame = sql(s"""
                       | source = $testTable | eval a = 1 | stats sum(a) as sum, count() as cnt, avg(a) as avg
                       | """.stripMargin)
    assertSameRows(Seq(Row(4, 4, 1.0)), frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val eval = Project(Seq(UnresolvedStar(None), Alias(Literal(1), "a")()), table)
    val sum =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "sum")()
    val avg =
      Alias(
        UnresolvedFunction(Seq("AVG"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "avg")()
    val count =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
        "cnt")()
    val aggregate = Aggregate(Seq.empty, Seq(sum, count, avg), eval)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregate)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test count() at the end of stats clause") {
    val frame = sql(s"""
                       | source = $testTable | eval a = 1 | stats sum(a) as sum, avg(a) as avg, count() as cnt
                       | """.stripMargin)
    assertSameRows(Seq(Row(4, 1.0, 4)), frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val eval = Project(Seq(UnresolvedStar(None), Alias(Literal(1), "a")()), table)
    val sum =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "sum")()
    val avg =
      Alias(
        UnresolvedFunction(Seq("AVG"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "avg")()
    val count =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
        "cnt")()
    val aggregate = Aggregate(Seq.empty, Seq(sum, avg, count), eval)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregate)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test count() at the first of stats by clause") {
    val frame = sql(s"""
                       | source = $testTable | eval a = 1 | stats count() as cnt, sum(a) as sum, avg(a) as avg by country
                       | """.stripMargin)
    assertSameRows(Seq(Row(2, 2, 1.0, "Canada"), Row(2, 2, 1.0, "USA")), frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val eval = Project(Seq(UnresolvedStar(None), Alias(Literal(1), "a")()), table)
    val sum =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "sum")()
    val avg =
      Alias(
        UnresolvedFunction(Seq("AVG"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "avg")()
    val count =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
        "cnt")()
    val grouping =
      Alias(UnresolvedAttribute("country"), "country")()
    val aggregate = Aggregate(Seq(grouping), Seq(count, sum, avg, grouping), eval)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregate)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test count() in the middle of stats by clause") {
    val frame = sql(s"""
                       | source = $testTable | eval a = 1 | stats sum(a) as sum, count() as cnt, avg(a) as avg by country
                       | """.stripMargin)
    assertSameRows(Seq(Row(2, 2, 1.0, "Canada"), Row(2, 2, 1.0, "USA")), frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val eval = Project(Seq(UnresolvedStar(None), Alias(Literal(1), "a")()), table)
    val sum =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "sum")()
    val avg =
      Alias(
        UnresolvedFunction(Seq("AVG"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "avg")()
    val count =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
        "cnt")()
    val grouping =
      Alias(UnresolvedAttribute("country"), "country")()
    val aggregate = Aggregate(Seq(grouping), Seq(sum, count, avg, grouping), eval)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregate)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test count() at the end of stats by clause") {
    val frame = sql(s"""
                       | source = $testTable | eval a = 1 | stats sum(a) as sum, avg(a) as avg, count() as cnt by country
                       | """.stripMargin)
    assertSameRows(Seq(Row(2, 1.0, 2, "Canada"), Row(2, 1.0, 2, "USA")), frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val eval = Project(Seq(UnresolvedStar(None), Alias(Literal(1), "a")()), table)
    val sum =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "sum")()
    val avg =
      Alias(
        UnresolvedFunction(Seq("AVG"), Seq(UnresolvedAttribute("a")), isDistinct = false),
        "avg")()
    val count =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
        "cnt")()
    val grouping =
      Alias(UnresolvedAttribute("country"), "country")()
    val aggregate = Aggregate(Seq(grouping), Seq(sum, avg, count, grouping), eval)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregate)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }
}
