/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import java.sql.Timestamp

import org.opensearch.flint.spark.FlintPPLSuite

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Divide, Floor, GenericRowWithSchema, Literal, Multiply, SortOrder, TimeWindow}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLTimeWindowITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "default.flint_ppl_sales_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    // Update table creation
    sql(s"""
         | CREATE TABLE $testTable
         | (
         |   transactionId STRING,
         |   transactionDate TIMESTAMP,
         |   productId STRING,
         |   productsAmount INT,
         |   customerId STRING
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
    // -- Inserting records into the testTable for April 2023
    sql(s"""
         |INSERT INTO $testTable PARTITION (year=2023, month=4)
         |VALUES
         |('txn001', CAST('2023-04-01 10:30:00' AS TIMESTAMP), 'prod1', 2, 'cust1'),
         |('txn001', CAST('2023-04-01 14:30:00' AS TIMESTAMP), 'prod1', 4, 'cust1'),
         |('txn002', CAST('2023-04-02 11:45:00' AS TIMESTAMP), 'prod2', 1, 'cust2'),
         |('txn003', CAST('2023-04-03 12:15:00' AS TIMESTAMP), 'prod3', 3, 'cust1'),
         |('txn004', CAST('2023-04-04 09:50:00' AS TIMESTAMP), 'prod1', 1, 'cust3')
         |  """.stripMargin)

    // Update data insertion
    // -- Inserting records into the testTable for May 2023
    sql(s"""
         |INSERT INTO $testTable PARTITION (year=2023, month=5)
         |VALUES
         |('txn005', CAST('2023-05-01 08:30:00' AS TIMESTAMP), 'prod2', 1, 'cust4'),
         |('txn006', CAST('2023-05-02 07:25:00' AS TIMESTAMP), 'prod4', 5, 'cust2'),
         |('txn007', CAST('2023-05-03 15:40:00' AS TIMESTAMP), 'prod3', 1, 'cust3'),
         |('txn007', CAST('2023-05-03 19:30:00' AS TIMESTAMP), 'prod3', 2, 'cust3'),
         |('txn008', CAST('2023-05-04 14:15:00' AS TIMESTAMP), 'prod1', 4, 'cust1')
         |  """.stripMargin)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("create ppl query count sales by days window test") {
    /*
       val dataFrame = spark.read.table(testTable)
       val query = dataFrame
         .groupBy(
           window(
             col("transactionDate"), " 1 days")
         ).agg(sum(col("productsAmount")))

       query.show(false)
     */
    val frame = sql(s"""
         | source = $testTable| stats sum(productsAmount) by span(transactionDate, 1d) as age_date
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame
      .collect()
      .map(row =>
        Row(
          row.get(0),
          row.getAs[GenericRowWithSchema](1).get(0),
          row.getAs[GenericRowWithSchema](1).get(1)))

    // Define the expected results
    val expectedResults = Array(
      Row(6, Timestamp.valueOf("2023-05-03 17:00:00"), Timestamp.valueOf("2023-05-04 17:00:00")),
      Row(3, Timestamp.valueOf("2023-04-02 17:00:00"), Timestamp.valueOf("2023-04-03 17:00:00")),
      Row(1, Timestamp.valueOf("2023-04-01 17:00:00"), Timestamp.valueOf("2023-04-02 17:00:00")),
      Row(1, Timestamp.valueOf("2023-04-03 17:00:00"), Timestamp.valueOf("2023-04-04 17:00:00")),
      Row(1, Timestamp.valueOf("2023-05-02 17:00:00"), Timestamp.valueOf("2023-05-03 17:00:00")),
      Row(5, Timestamp.valueOf("2023-05-01 17:00:00"), Timestamp.valueOf("2023-05-02 17:00:00")),
      Row(1, Timestamp.valueOf("2023-04-30 17:00:00"), Timestamp.valueOf("2023-05-01 17:00:00")),
      Row(6, Timestamp.valueOf("2023-03-31 17:00:00"), Timestamp.valueOf("2023-04-01 17:00:00")))
    // Compare the results
    implicit val timestampOrdering: Ordering[Timestamp] = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
    }

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Timestamp](_.getAs[Timestamp](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val productsAmount = UnresolvedAttribute("productsAmount")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_sales_test"))

    val windowExpression = Alias(
      TimeWindow(
        UnresolvedAttribute("transactionDate"),
        TimeWindow.parseExpression(Literal("1 day")),
        TimeWindow.parseExpression(Literal("1 day")),
        0),
      "age_date")()

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(productsAmount), isDistinct = false),
        "sum(productsAmount)")()
    val aggregatePlan =
      Aggregate(Seq(windowExpression), Seq(aggregateExpressions, windowExpression), table)
    val expectedPlan = Project(star, aggregatePlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl query count sales by days window with sorting test") {
    val frame = sql(s"""
         | source = $testTable| stats sum(productsAmount) by span(transactionDate, 1d) as age_date | sort age_date
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame
      .collect()
      .map(row =>
        Row(
          row.get(0),
          row.getAs[GenericRowWithSchema](1).get(0),
          row.getAs[GenericRowWithSchema](1).get(1)))

    // Define the expected results
    val expectedResults = Array(
      Row(6, Timestamp.valueOf("2023-05-03 17:00:00"), Timestamp.valueOf("2023-05-04 17:00:00")),
      Row(3, Timestamp.valueOf("2023-04-02 17:00:00"), Timestamp.valueOf("2023-04-03 17:00:00")),
      Row(1, Timestamp.valueOf("2023-04-01 17:00:00"), Timestamp.valueOf("2023-04-02 17:00:00")),
      Row(1, Timestamp.valueOf("2023-04-03 17:00:00"), Timestamp.valueOf("2023-04-04 17:00:00")),
      Row(1, Timestamp.valueOf("2023-05-02 17:00:00"), Timestamp.valueOf("2023-05-03 17:00:00")),
      Row(5, Timestamp.valueOf("2023-05-01 17:00:00"), Timestamp.valueOf("2023-05-02 17:00:00")),
      Row(1, Timestamp.valueOf("2023-04-30 17:00:00"), Timestamp.valueOf("2023-05-01 17:00:00")),
      Row(6, Timestamp.valueOf("2023-03-31 17:00:00"), Timestamp.valueOf("2023-04-01 17:00:00")))
    // Compare the results
    implicit val timestampOrdering: Ordering[Timestamp] = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
    }

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Timestamp](_.getAs[Timestamp](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val productsAmount = UnresolvedAttribute("productsAmount")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_sales_test"))

    val windowExpression = Alias(
      TimeWindow(
        UnresolvedAttribute("transactionDate"),
        TimeWindow.parseExpression(Literal("1 day")),
        TimeWindow.parseExpression(Literal("1 day")),
        0),
      "age_date")()

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(productsAmount), isDistinct = false),
        "sum(productsAmount)")()
    val aggregatePlan =
      Aggregate(Seq(windowExpression), Seq(aggregateExpressions, windowExpression), table)
    val sortedPlan: LogicalPlan = Sort(
      Seq(SortOrder(UnresolvedAttribute("age_date"), Ascending)),
      global = true,
      aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl query count sales by days window and productId with sorting test") {
    val frame = sql(s"""
         | source = $testTable| stats sum(productsAmount) by span(transactionDate, 1d) as age_date, productId | sort age_date
         | """.stripMargin)

    frame.show(false)
    // Retrieve the results
    val results: Array[Row] = frame
      .collect()
      .map(row =>
        Row(
          row.get(0),
          row.get(1),
          row.getAs[GenericRowWithSchema](2).get(0),
          row.getAs[GenericRowWithSchema](2).get(1)))

    // Define the expected results
    val expectedResults = Array(
      Row(
        6,
        "prod1",
        Timestamp.valueOf("2023-03-31 17:00:00"),
        Timestamp.valueOf("2023-04-01 17:00:00")),
      Row(
        1,
        "prod2",
        Timestamp.valueOf("2023-04-01 17:00:00"),
        Timestamp.valueOf("2023-04-02 17:00:00")),
      Row(
        3,
        "prod3",
        Timestamp.valueOf("2023-04-02 17:00:00"),
        Timestamp.valueOf("2023-04-03 17:00:00")),
      Row(
        1,
        "prod1",
        Timestamp.valueOf("2023-04-03 17:00:00"),
        Timestamp.valueOf("2023-04-04 17:00:00")),
      Row(
        1,
        "prod2",
        Timestamp.valueOf("2023-04-30 17:00:00"),
        Timestamp.valueOf("2023-05-01 17:00:00")),
      Row(
        5,
        "prod4",
        Timestamp.valueOf("2023-05-01 17:00:00"),
        Timestamp.valueOf("2023-05-02 17:00:00")),
      Row(
        1,
        "prod3",
        Timestamp.valueOf("2023-05-02 17:00:00"),
        Timestamp.valueOf("2023-05-03 17:00:00")),
      Row(
        4,
        "prod1",
        Timestamp.valueOf("2023-05-03 17:00:00"),
        Timestamp.valueOf("2023-05-04 17:00:00")),
      Row(
        2,
        "prod3",
        Timestamp.valueOf("2023-05-03 17:00:00"),
        Timestamp.valueOf("2023-05-04 17:00:00")))
    // Compare the results
    implicit val timestampOrdering: Ordering[Timestamp] = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
    }

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Timestamp](_.getAs[Timestamp](2))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val productsId = Alias(UnresolvedAttribute("productId"), "productId")()
    val productsAmount = UnresolvedAttribute("productsAmount")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_sales_test"))

    val windowExpression = Alias(
      TimeWindow(
        UnresolvedAttribute("transactionDate"),
        TimeWindow.parseExpression(Literal("1 day")),
        TimeWindow.parseExpression(Literal("1 day")),
        0),
      "age_date")()

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(productsAmount), isDistinct = false),
        "sum(productsAmount)")()
    val aggregatePlan = Aggregate(
      Seq(productsId, windowExpression),
      Seq(aggregateExpressions, productsId, windowExpression),
      table)
    val sortedPlan: LogicalPlan = Sort(
      Seq(SortOrder(UnresolvedAttribute("age_date"), Ascending)),
      global = true,
      aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }
  test("create ppl query count sales by weeks window and productId with sorting test") {
    val frame = sql(s"""
         | source = $testTable| stats sum(productsAmount) by span(transactionDate, 1w) as age_date | sort age_date
         | """.stripMargin)

    frame.show(false)
    // Retrieve the results
    val results: Array[Row] = frame
      .collect()
      .map(row =>
        Row(
          row.get(0),
          row.getAs[GenericRowWithSchema](1).get(0),
          row.getAs[GenericRowWithSchema](1).get(1)))

    // Define the expected results
    val expectedResults = Array(
      Row(11, Timestamp.valueOf("2023-03-29 17:00:00"), Timestamp.valueOf("2023-04-05 17:00:00")),
      Row(7, Timestamp.valueOf("2023-04-26 17:00:00"), Timestamp.valueOf("2023-05-03 17:00:00")),
      Row(6, Timestamp.valueOf("2023-05-03 17:00:00"), Timestamp.valueOf("2023-05-10 17:00:00")))

    // Compare the results
    implicit val timestampOrdering: Ordering[Timestamp] = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
    }

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Timestamp](_.getAs[Timestamp](1))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val star = Seq(UnresolvedStar(None))
    val productsAmount = UnresolvedAttribute("productsAmount")
    val table = UnresolvedRelation(Seq("default", "flint_ppl_sales_test"))

    val windowExpression = Alias(
      TimeWindow(
        UnresolvedAttribute("transactionDate"),
        TimeWindow.parseExpression(Literal("1 week")),
        TimeWindow.parseExpression(Literal("1 week")),
        0),
      "age_date")()

    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("SUM"), Seq(productsAmount), isDistinct = false),
        "sum(productsAmount)")()
    val aggregatePlan =
      Aggregate(Seq(windowExpression), Seq(aggregateExpressions, windowExpression), table)
    val sortedPlan: LogicalPlan = Sort(
      Seq(SortOrder(UnresolvedAttribute("age_date"), Ascending)),
      global = true,
      aggregatePlan)
    val expectedPlan = Project(star, sortedPlan)
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
}
