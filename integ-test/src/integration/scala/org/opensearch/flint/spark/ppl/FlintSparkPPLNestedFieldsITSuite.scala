/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedExtractValue, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, EqualTo, GreaterThan, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLNestedFieldsITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val nestedTestTable = "spark_catalog.default.flint_ppl_test_nested"
  private val nestedTestTableWithNestedKeys =
    "spark_catalog.default.flint_ppl_test_nested_with_nested_keys"

  override def beforeAll(): Unit = {
    super.beforeAll()

    createStructNestedTable(nestedTestTable)
    createStructNestedTableWithKeysLikeNestedFields(nestedTestTableWithNestedKeys)
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
    val testTableQuoted = "`spark_catalog`.`default`.`flint_ppl_test_nested`"
    Seq(nestedTestTable, testTableQuoted).foreach { table =>
      val frame = sql(s"""
           | source = $table
           | """.stripMargin)

      // Retrieve the results
      val results: Array[Row] = frame.collect()
      // Define the expected results
      val expectedResults: Array[Row] = Array(
        Row(30, Row(Row("value1"), 123), Row(Row("valueA"), 23)),
        Row(40, Row(Row("value5"), 123), Row(Row("valueB"), 33)),
        Row(30, Row(Row("value4"), 823), Row(Row("valueC"), 83)),
        Row(40, Row(Row("value2"), 456), Row(Row("valueD"), 46)),
        Row(50, Row(Row("value3"), 789), Row(Row("valueE"), 89)))
      // Compare the results
      implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
      assert(results.sorted.sameElements(expectedResults.sorted))

      // Retrieve the logical plan
      val logicalPlan: LogicalPlan = frame.queryExecution.logical
      // Define the expected logical plan
      val expectedPlan: LogicalPlan =
        Project(
          Seq(UnresolvedStar(None)),
          UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_nested")))
      // Compare the two plans
      assert(expectedPlan === logicalPlan)
    }
  }

  test("create ppl simple query with head (limit) 1 test") {
    val frame = sql(s"""
         | source = $nestedTestTable| head 1
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val limitPlan: LogicalPlan =
      Limit(
        Literal(1),
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_nested")))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), limitPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query with head (limit) and sorted test") {
    val frame = sql(s"""
         | source = $nestedTestTable| sort int_col | head 1
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("int_col"), Ascending)),
        global = true,
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_nested")))

    // Define the expected logical plan
    val expectedPlan: LogicalPlan =
      Project(Seq(UnresolvedStar(None)), Limit(Literal(1), sortedPlan))

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query with head (limit) and nested column sorted test") {
    val frame = sql(s"""
         | source = $nestedTestTable| sort struct_col.field1 | head 1
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("struct_col.field1"), Ascending)),
        global = true,
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_nested")))

    // Define the expected logical plan
    val expectedPlan: LogicalPlan =
      Project(Seq(UnresolvedStar(None)), Limit(Literal(1), sortedPlan))

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query two with fields result test") {
    val frame = sql(s"""
         | source = $nestedTestTable| fields int_col, struct_col.field2, struct_col2.field2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(
        Row(30, 123, 23),
        Row(30, 823, 83),
        Row(40, 123, 33),
        Row(40, 456, 46),
        Row(50, 789, 89))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Project(
      Seq(
        UnresolvedAttribute("int_col"),
        UnresolvedAttribute("struct_col.field2"),
        UnresolvedAttribute("struct_col2.field2")),
      UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_nested")))
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }

  test("create ppl simple sorted query two with fields result test sorted") {
    val frame = sql(s"""
         | source = $nestedTestTable| sort - struct_col2.field2 | fields int_col, struct_col2.field2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row(50, 89), Row(30, 83), Row(40, 46), Row(40, 33), Row(30, 23))
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("struct_col2.field2"), Descending)),
        global = true,
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_nested")))

    // Define the expected logical plan
    val expectedPlan: LogicalPlan =
      Project(
        Seq(UnresolvedAttribute("int_col"), UnresolvedAttribute("struct_col2.field2")),
        sortedPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple sorted by nested field query with two with fields result test ") {
    val frame = sql(s"""
         | source = $nestedTestTable| sort - struct_col.field2 , - int_col | fields int_col, struct_col.field2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row(30, 823), Row(50, 789), Row(40, 456), Row(40, 123), Row(30, 123))
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(
          SortOrder(UnresolvedAttribute("struct_col.field2"), Descending),
          SortOrder(UnresolvedAttribute("int_col"), Descending)),
        global = true,
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_nested")))

    // Define the expected logical plan
    val expectedPlan: LogicalPlan =
      Project(
        Seq(UnresolvedAttribute("int_col"), UnresolvedAttribute("struct_col.field2")),
        sortedPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query with nested field 1 range filter test") {
    val frame = sql(s"""
         | source = $nestedTestTable| where struct_col.field2 > 200 | sort  - struct_col.field2 | fields  int_col, struct_col.field2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row(30, 823), Row(50, 789), Row(40, 456))
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_nested"))
    // Define the expected logical plan components
    val filterPlan =
      Filter(GreaterThan(UnresolvedAttribute("struct_col.field2"), Literal(200)), table)
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("struct_col.field2"), Descending)),
        global = true,
        filterPlan)
    val expectedPlan =
      Project(
        Seq(UnresolvedAttribute("int_col"), UnresolvedAttribute("struct_col.field2")),
        sortedPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query with nested field 2 range filter test") {
    val frame = sql(s"""
         | source = $nestedTestTable| where struct_col2.field2 > 50 |  sort - struct_col2.field2 | fields  int_col, struct_col2.field2
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row(50, 89), Row(30, 83))
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_nested"))
    // Define the expected logical plan components
    val filterPlan =
      Filter(GreaterThan(UnresolvedAttribute("struct_col2.field2"), Literal(50)), table)
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("struct_col2.field2"), Descending)),
        global = true,
        filterPlan)
    val expectedPlan =
      Project(
        Seq(UnresolvedAttribute("int_col"), UnresolvedAttribute("struct_col2.field2")),
        sortedPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query with nested field string match test") {
    val frame = sql(s"""
                       | source = $nestedTestTable| where struct_col.field1.subfield = 'value1' | sort int_col | fields  int_col, struct_col.field1.subfield, struct_col2.field1.subfield
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row(30, "value1", "valueA"))
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_nested"))
    // Define the expected logical plan components
    val filterPlan =
      Filter(EqualTo(UnresolvedAttribute("struct_col.field1.subfield"), Literal("value1")), table)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("int_col"), Ascending)), global = true, filterPlan)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("int_col"),
          UnresolvedAttribute("struct_col.field1.subfield"),
          UnresolvedAttribute("struct_col2.field1.subfield")),
        sortedPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query with nested field string filter test") {
    val frame = sql(s"""
                       | source = $nestedTestTable| where struct_col2.field1.subfield > 'valueA' | sort int_col | fields  int_col, struct_col.field1.subfield, struct_col2.field1.subfield
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(
        Row(30, "value4", "valueC"),
        Row(40, "value5", "valueB"),
        Row(40, "value2", "valueD"),
        Row(50, "value3", "valueE"))
    assert(results === expectedResults)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_nested"))
    // Define the expected logical plan components
    val filterPlan = Filter(
      GreaterThan(UnresolvedAttribute("struct_col2.field1.subfield"), Literal("valueA")),
      table)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("int_col"), Ascending)), global = true, filterPlan)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("int_col"),
          UnresolvedAttribute("struct_col.field1.subfield"),
          UnresolvedAttribute("struct_col2.field1.subfield")),
        sortedPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query nested column with dots in name test") {
    val frame = sql(s"""
                          | source = $nestedTestTableWithNestedKeys | fields user_data[user.first.name]
                          | """.stripMargin)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row("Alice"), Row("Bob"), Row("Charlie"), Row("David"))
    assert(results.length == 4)
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val pplLogicalPlan: LogicalPlan = frame.queryExecution.logical

    // Define the expected logical plan
    val table = UnresolvedRelation(
      Seq("spark_catalog", "default", "flint_ppl_test_nested_with_nested_keys"))
    val expectedPlan = Project(
      Seq(
        UnresolvedAlias(
          UnresolvedExtractValue(UnresolvedAttribute("user_data"), Literal("user.first.name")))),
      table)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(pplLogicalPlan))
  }

  test("create ppl simple query nested column with dots in name filter test") {
    val frame = sql(s"""
                          | source = $nestedTestTableWithNestedKeys | where user_data[user.home.address.city] = 'Seattle'
                          | """.stripMargin)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row(Row("Alice", "Smith", 30, "123 Main St", "Seattle"), Row("asmith", "REDACTED")),
      Row(Row("Bob", "Johnson", 55, "456 Elm St", "Seattle"), Row("bjohnson", "REDACTED")))
    assert(results.length == 2)
    // Compare the results
    assert(results === expectedResults)

    // Retrieve the logical plan
    val pplLogicalPlan: LogicalPlan = frame.queryExecution.logical

    // Define the expected logical plan
    val table = UnresolvedRelation(
      Seq("spark_catalog", "default", "flint_ppl_test_nested_with_nested_keys"))
    val cityEqualTo = EqualTo(
      UnresolvedExtractValue(UnresolvedAttribute("user_data"), Literal("user.home.address.city")),
      Literal("Seattle"))
    val filter = Filter(cityEqualTo, table)
    val expectedPlan = Project(Seq(UnresolvedStar(Option.empty)), filter)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(pplLogicalPlan))
  }
}
