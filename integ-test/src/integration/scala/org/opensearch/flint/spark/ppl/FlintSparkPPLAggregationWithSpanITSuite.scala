/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Descending, Divide, EqualTo, Floor, Literal, Multiply, Not, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLAggregationWithSpanITSuite
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
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("COUNT"), Seq(ageField), isDistinct = false), "count(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
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
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
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
    val ageField = UnresolvedAttribute("age")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(ageField), isDistinct = false), "avg(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), table)
    val limitPlan = Limit(Literal(2), aggregatePlan)
    val expectedPlan = Project(star, limitPlan)

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

  test(
    "create ppl average age by span of interval of 10 years group by country head (limit) 2 query test ") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) by span(age, 10) as age_span, country | head 3
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
    val limitPlan = Limit(Literal(3), aggregatePlan)
    val expectedPlan = Project(star, limitPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test(
    "create ppl average age by span of interval of 10 years group by country head (limit) 2 query and sort by test ") {
    val frame = sql(s"""
         | source = $testTable| stats avg(age) by span(age, 10) as age_span, country  | sort - age_span |  head 2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 2)

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
    val sortedPlan: LogicalPlan = Sort(
      Seq(SortOrder(UnresolvedAttribute("age_span"), Descending)),
      global = true,
      aggregatePlan)
    val limitPlan = Limit(Literal(2), sortedPlan)
    val expectedPlan = Project(star, limitPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  /**
   * | age_span |    age_stddev_samp |
   * |:---------|-------------------:|
   * | 20       | 3.5355339059327378 |
   */
  test(
    "create ppl age sample stddev by span of interval of 10 years query with country filter test ") {
    val frame = sql(s"""
                       | source = $testTable | where country != 'USA' | stats stddev_samp(age) by span(age, 10) as age_span
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(3.5355339059327378d, 20L))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Long](_.getAs[Long](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val countryField = UnresolvedAttribute("country")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("STDDEV_SAMP"), Seq(ageField), isDistinct = false),
        "stddev_samp(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val filterExpr = Not(EqualTo(countryField, Literal("USA")))
    val filterPlan = Filter(filterExpr, table)
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  /**
   * | age_span | age_stddev_pop |
   * |:---------|---------------:|
   * | 20       |            2.5 |
   * | 30       |              0 |
   */
  test(
    "create ppl age population stddev by span of interval of 10 years query with state filter test ") {
    val frame = sql(s"""
                       | source = $testTable | where state != 'California' | stats stddev_pop(age) by span(age, 10) as age_span
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(2.5d, 20L), Row(0d, 30L))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Long](_.getAs[Long](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val stateField = UnresolvedAttribute("state")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("STDDEV_POP"), Seq(ageField), isDistinct = false),
        "stddev_pop(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val filterExpr = Not(EqualTo(stateField, Literal("California")))
    val filterPlan = Filter(filterExpr, table)
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  /**
   * | age_span | age_percentile |
   * |:---------|---------------:|
   * | 20       |             25 |
   * | 30       |             30 |
   * | 70       |             70 |
   */
  test(
    "create ppl simple age 60th percentile by span of interval of 10 years query with state filter test ") {
    val frame = sql(s"""
                       | source = $testTable | where state != 'Quebec' | stats percentile(age, 60) by span(age, 10) as age_span
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(70d, 70L), Row(30d, 30L), Row(25d, 20L))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Long](_.getAs[Long](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val percentage = Literal(0.6)
    val stateField = UnresolvedAttribute("state")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("PERCENTILE"), Seq(ageField, percentage), isDistinct = false),
        "percentile(age, 60)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val filterExpr = Not(EqualTo(stateField, Literal("Quebec")))
    val filterPlan = Filter(filterExpr, table)
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  /**
   * | age_span | age_percentile |
   * |:---------|---------------:|
   * | 20       |             25 |
   * | 30       |             30 |
   * | 70       |             70 |
   */
  test(
    "create ppl simple age 60th percentile approx by span of interval of 10 years query with state filter test ") {
    val frame = sql(s"""
                       | source = $testTable | where state != 'Quebec' | stats percentile_approx(age, 60) by span(age, 10) as age_span
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(70d, 70L), Row(30d, 30L), Row(25d, 20L))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Long](_.getAs[Long](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val percentage = Literal(0.6)
    val stateField = UnresolvedAttribute("state")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(
          Seq("PERCENTILE_APPROX"),
          Seq(ageField, percentage),
          isDistinct = false),
        "percentile_approx(age, 60)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val filterExpr = Not(EqualTo(stateField, Literal("Quebec")))
    val filterPlan = Filter(filterExpr, table)
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  /**
   * | age_span | count_age |
   * |:---------|----------:|
   * | 20       |         1 |
   * | 30       |         1 |
   * | 70       |         1 |
   */
  test(
    "create ppl simple distinct count age by span of interval of 10 years query with state filter test ") {
    val frame = sql(s"""
                       | source = $testTable | where state != 'Quebec' | stats distinct_count(age) by span(age, 10) as age_span
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(1, 70L), Row(1, 30L), Row(1, 20L))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Long](_.getAs[Long](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val stateField = UnresolvedAttribute("state")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(ageField), isDistinct = true),
        "distinct_count(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val filterExpr = Not(EqualTo(stateField, Literal("Quebec")))
    val filterPlan = Filter(filterExpr, table)
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    comparePlans(expectedPlan, logicalPlan, false)
  }

  test(
    "create ppl simple distinct count age by span of interval of 10 years query with state filter test using approximation") {
    val frame = sql(s"""
                       | source = $testTable | where state != 'Quebec' | stats distinct_count_approx(age) by span(age, 10) as age_span
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(1, 70L), Row(1, 30L), Row(1, 20L))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Long](_.getAs[Long](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val ageField = UnresolvedAttribute("age")
    val stateField = UnresolvedAttribute("state")
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("APPROX_COUNT_DISTINCT"), Seq(ageField), isDistinct = true),
        "distinct_count_approx(age)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val filterExpr = Not(EqualTo(stateField, Literal("Quebec")))
    val filterPlan = Filter(filterExpr, table)
    val aggregatePlan = Aggregate(Seq(span), Seq(aggregateExpressions, span), filterPlan)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    comparePlans(expectedPlan, logicalPlan, false)
  }
}
