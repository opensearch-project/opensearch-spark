/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, CaseWhen, Descending, EqualTo, GreaterThanOrEqual, LessThan, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project, Sort}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLEvalITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"
  private val testTableHttpLog = "spark_catalog.default.flint_ppl_test_http_log"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createPartitionedStateCountryTable(testTable)
    createTableHttpLog(testTableHttpLog)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test single eval expression with new field") {
    val frame = sql(s"""
         | source = $testTable | eval col = 1 | fields name, age
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
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val evalProjectList = Seq(UnresolvedStar(None), Alias(Literal(1), "col")())
    val expectedPlan = Project(fieldsProjectList, Project(evalProjectList, table))
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test multiple eval expressions with new fields") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = 1, col2 = 2 | fields name, age
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] =
      Array(Row("Jake", 70), Row("Hello", 30), Row("John", 25), Row("Jane", 20))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val evalProjectList =
      Seq(UnresolvedStar(None), Alias(Literal(1), "col1")(), Alias(Literal(2), "col2")())
    val expectedPlan = Project(fieldsProjectList, Project(evalProjectList, table))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test eval expressions in fields command") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = 1, col2 = 2 | fields name, age, col1, col2
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, 1, 2),
      Row("Hello", 30, 1, 2),
      Row("John", 25, 1, 2),
      Row("Jane", 20, 1, 2))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList = Seq(
      UnresolvedAttribute("name"),
      UnresolvedAttribute("age"),
      UnresolvedAttribute("col1"),
      UnresolvedAttribute("col2"))
    val evalProjectList =
      Seq(UnresolvedStar(None), Alias(Literal(1), "col1")(), Alias(Literal(2), "col2")())
    val expectedPlan = Project(fieldsProjectList, Project(evalProjectList, table))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test eval expression without fields command") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = "New Field1", col2 = "New Field2"
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, "California", "USA", 2023, 4, "New Field1", "New Field2"),
      Row("Hello", 30, "New York", "USA", 2023, 4, "New Field1", "New Field2"),
      Row("John", 25, "Ontario", "Canada", 2023, 4, "New Field1", "New Field2"),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, "New Field1", "New Field2"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val projectList = Seq(
      UnresolvedStar(None),
      Alias(Literal("New Field1"), "col1")(),
      Alias(Literal("New Field2"), "col2")())
    val expectedPlan = Project(seq(UnresolvedStar(None)), Project(projectList, table))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test reusing existing fields in eval expressions") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = state, col2 = country | fields name, age, col1, col2
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, "California", "USA"),
      Row("Hello", 30, "New York", "USA"),
      Row("John", 25, "Ontario", "Canada"),
      Row("Jane", 20, "Quebec", "Canada"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList = Seq(
      UnresolvedAttribute("name"),
      UnresolvedAttribute("age"),
      UnresolvedAttribute("col1"),
      UnresolvedAttribute("col2"))
    val evalProjectList = Seq(
      UnresolvedStar(None),
      Alias(UnresolvedAttribute("state"), "col1")(),
      Alias(UnresolvedAttribute("country"), "col2")())
    val expectedPlan = Project(fieldsProjectList, Project(evalProjectList, table))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test(
    "test overriding existing fields: throw exception when specify the new field in fields command") {
    val ex = intercept[AnalysisException](sql(s"""
         | source = $testTable | eval age = 40 | eval name = upper(name) | sort name | fields name, age, state
         | """.stripMargin))
    assert(ex.getMessage().contains("Reference `name` is ambiguous"))
  }

  test("test overriding existing fields: throw exception when specify the new field in where") {
    val ex = intercept[AnalysisException](sql(s"""
         | source = $testTable | eval age = abs(age) | where age < 50
         | """.stripMargin))
    assert(ex.getMessage().contains("Reference `age` is ambiguous"))
  }

  test(
    "test overriding existing fields: throw exception when specify the new field in aggregate expression") {
    val ex = intercept[AnalysisException](sql(s"""
         | source = $testTable | eval age = abs(age) | stats avg(age)
         | """.stripMargin))
    assert(ex.getMessage().contains("Reference `age` is ambiguous"))
  }

  test(
    "test overriding existing fields: throw exception when specify the new field in grouping list") {
    val ex = intercept[AnalysisException](sql(s"""
         | source = $testTable | eval country = upper(country) | stats avg(age) by country
         | """.stripMargin))
    assert(ex.getMessage().contains("Reference `country` is ambiguous"))
  }

  test("test override existing fields: the eval field doesn't appear in fields command") {
    val frame = sql(s"""
         | source = $testTable | eval age = 40, name = upper(name) | sort name | fields state, country
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("New York", "USA"),
      Row("California", "USA"),
      Row("Quebec", "Canada"),
      Row("Ontario", "Canada"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val projectList = Seq(
      UnresolvedStar(None),
      Alias(Literal(40), "age")(),
      Alias(
        UnresolvedFunction("upper", seq(UnresolvedAttribute("name")), isDistinct = false),
        "name")())
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("name"), Ascending)),
        global = true,
        Project(projectList, table))
    val expectedPlan =
      Project(seq(UnresolvedAttribute("state"), UnresolvedAttribute("country")), sortedPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test override existing fields: the new fields not appear in fields command") {
    val frame = sql(s"""
         | source = $testTable | eval age = 40 | eval name = upper(name) | sort name
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    // In Spark, `name` in eval (as an alias) will be treated as a new column (exprIds are different).
    // So if `name` appears in fields command, it will throw ambiguous reference exception.
    val expectedResults: Array[Row] = Array(
      Row("Hello", 30, "New York", "USA", 2023, 4, 40, "HELLO"),
      Row("Jake", 70, "California", "USA", 2023, 4, 40, "JAKE"),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, 40, "JANE"),
      Row("John", 25, "Ontario", "Canada", 2023, 4, 40, "JOHN"))
    assert(results.sameElements(expectedResults))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val evalProjectList1 = Seq(UnresolvedStar(None), Alias(Literal(40), "age")())
    val evalProjectList2 = Seq(
      UnresolvedStar(None),
      Alias(
        UnresolvedFunction("upper", seq(UnresolvedAttribute("name")), isDistinct = false),
        "name")())
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("name"), Ascending)),
        global = true,
        Project(evalProjectList2, Project(evalProjectList1, table)))
    val expectedPlan = Project(seq(UnresolvedStar(None)), sortedPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test multiple eval commands in fields list") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = 1 | eval col2 = 2 | fields name, age, col1, col2
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, 1, 2),
      Row("Hello", 30, 1, 2),
      Row("John", 25, 1, 2),
      Row("Jane", 20, 1, 2))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList = Seq(
      UnresolvedAttribute("name"),
      UnresolvedAttribute("age"),
      UnresolvedAttribute("col1"),
      UnresolvedAttribute("col2"))
    val evalProjectList1 = Seq(UnresolvedStar(None), Alias(Literal(1), "col1")())
    val evalProjectList2 = Seq(UnresolvedStar(None), Alias(Literal(2), "col2")())
    val expectedPlan =
      Project(fieldsProjectList, Project(evalProjectList2, Project(evalProjectList1, table)))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test multiple eval commands without fields command") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = ln(age) | eval col2 = unix_timestamp('2020-09-16 17:30:00') | sort - col1
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, "California", "USA", 2023, 4, 4.248495242049359, 1600302600),
      Row("Hello", 30, "New York", "USA", 2023, 4, 3.4011973816621555, 1600302600),
      Row("John", 25, "Ontario", "Canada", 2023, 4, 3.2188758248682006, 1600302600),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, 2.995732273553991, 1600302600))
    assert(results.sameElements(expectedResults))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val evalProjectList1 = Seq(
      UnresolvedStar(None),
      Alias(
        UnresolvedFunction("ln", seq(UnresolvedAttribute("age")), isDistinct = false),
        "col1")())
    val evalProjectList2 = Seq(
      UnresolvedStar(None),
      Alias(
        UnresolvedFunction(
          "unix_timestamp",
          seq(Literal("2020-09-16 17:30:00")),
          isDistinct = false),
        "col2")())
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("col1"), Descending)),
        global = true,
        Project(evalProjectList2, Project(evalProjectList1, table)))
    val expectedPlan = Project(seq(UnresolvedStar(None)), sortedPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test complex eval commands - case 1") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = 1 | sort col1 | head 4 | eval col2 = 2 | sort - col2 | sort age | head 2 | fields name, age, col2
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(Row("Jane", 20, 2), Row("John", 25, 2))
    assert(results.sameElements(expectedResults))
  }

  test("test complex eval commands - case 2") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = age | sort - col1 | head 3 | eval col2 = age | sort + col2 | head 2
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("John", 25, "Ontario", "Canada", 2023, 4, 25, 25),
      Row("Hello", 30, "New York", "USA", 2023, 4, 30, 30))
    assert(results.sameElements(expectedResults))
  }

  test("test complex eval commands - case 3") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = age | sort - col1 | head 3 | eval col2 = age | fields name, age | sort + col2 | head 2
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(Row("John", 25), Row("Hello", 30))
    assert(results.sameElements(expectedResults))
  }

  test("test complex eval commands - case 4: execute 1, 2 and 3 together") {
    val frame1 = sql(s"""
         | source = $testTable | eval col1 = 1 | sort col1 | head 4 | eval col2 = 2 | sort - col2 | sort age | head 2 | fields name, age, col2
         | """.stripMargin)
    val results1: Array[Row] = frame1.collect()
    // results1.foreach(println(_))
    val expectedResults1: Array[Row] = Array(Row("Jane", 20, 2), Row("John", 25, 2))
    assert(results1.sameElements(expectedResults1))

    val frame2 = sql(s"""
         | source = $testTable | eval col1 = age | sort - col1 | head 3 | eval col2 = age | sort + col2 | head 2
         | """.stripMargin)
    val results2: Array[Row] = frame2.collect()
    // results2.foreach(println(_))
    val expectedResults2: Array[Row] = Array(
      Row("John", 25, "Ontario", "Canada", 2023, 4, 25, 25),
      Row("Hello", 30, "New York", "USA", 2023, 4, 30, 30))
    assert(results2.sameElements(expectedResults2))

    val frame3 = sql(s"""
         | source = $testTable | eval col1 = age | sort - col1 | head 3 | eval col2 = age | fields name, age | sort + col2 | head 2
         | """.stripMargin)
    val results3: Array[Row] = frame3.collect()
    // results3.foreach(println(_))
    val expectedResults3: Array[Row] = Array(Row("John", 25), Row("Hello", 30))
    assert(results3.sameElements(expectedResults3))
  }

  test("test eval expression used in aggregation") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = age, col2 = country | stats avg(col1) by col2
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(Row(22.5, "Canada"), Row(50.0, "USA"))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val evalProjectList = Seq(
      UnresolvedStar(None),
      Alias(UnresolvedAttribute("age"), "col1")(),
      Alias(UnresolvedAttribute("country"), "col2")())
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val aggregateExpressions =
      Seq(
        Alias(
          UnresolvedFunction(Seq("AVG"), Seq(UnresolvedAttribute("col1")), isDistinct = false),
          "avg(col1)")(),
        Alias(UnresolvedAttribute("col2"), "col2")())
    val aggregatePlan = Aggregate(
      Seq(Alias(UnresolvedAttribute("col2"), "col2")()),
      aggregateExpressions,
      Project(evalProjectList, table))
    val expectedPlan = Project(seq(UnresolvedStar(None)), aggregatePlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test complex eval expressions with fields command") {
    val frame = sql(s"""
         | source = $testTable | eval new_name = upper(name) | eval compound_field = concat('Hello ', if(like(new_name, 'HEL%'), 'World', name)) | fields new_name, compound_field
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("JAKE", "Hello Jake"),
      Row("HELLO", "Hello World"),
      Row("JOHN", "Hello John"),
      Row("JANE", "Hello Jane"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test complex eval expressions without fields command") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = "New Field" | eval col2 = upper(lower(col1))
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, "California", "USA", 2023, 4, "New Field", "NEW FIELD"),
      Row("Hello", 30, "New York", "USA", 2023, 4, "New Field", "NEW FIELD"),
      Row("John", 25, "Ontario", "Canada", 2023, 4, "New Field", "NEW FIELD"),
      Row("Jane", 20, "Quebec", "Canada", 2023, 4, "New Field", "NEW FIELD"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test depended eval expressions in individual eval command") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = 1 | eval col2 = col1 | fields name, age, col2
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] =
      Array(Row("Jake", 70, 1), Row("Hello", 30, 1), Row("John", 25, 1), Row("Jane", 20, 1))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList =
      Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"), UnresolvedAttribute("col2"))
    val evalProjectList1 = Seq(UnresolvedStar(None), Alias(Literal(1), "col1")())
    val evalProjectList2 = Seq(UnresolvedStar(None), Alias(UnresolvedAttribute("col1"), "col2")())
    val expectedPlan =
      Project(fieldsProjectList, Project(evalProjectList2, Project(evalProjectList1, table)))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test lateral eval expressions references") {
    val frame = sql(s"""
         | source = $testTable | eval col1 = 1, col2 = col1 | fields name, age, col2
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] =
      Array(Row("Jake", 70, 1), Row("Hello", 30, 1), Row("John", 25, 1), Row("Jane", 20, 1))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList =
      Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"), UnresolvedAttribute("col2"))
    val evalProjectList = Seq(
      UnresolvedStar(None),
      Alias(Literal(1), "col1")(),
      Alias(UnresolvedAttribute("col1"), "col2")())
    val expectedPlan = Project(fieldsProjectList, Project(evalProjectList, table))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("eval case function") {
    val frame = sql(s"""
                       | source = $testTableHttpLog |
                       | eval status_category =
                       | case(status_code >= 200 AND status_code < 300, 'Success',
                       |  status_code >= 300 AND status_code < 400, 'Redirection',
                       |  status_code >= 400 AND status_code < 500, 'Client Error',
                       |  status_code >= 500, 'Server Error'
                       |  else 'Unknown'
                       | )
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(1, 200, "/home", "2023-10-01 10:00:00", "Success"),
        Row(2, null, "/about", "2023-10-01 10:05:00", "Unknown"),
        Row(3, 500, "/contact", "2023-10-01 10:10:00", "Server Error"),
        Row(4, 301, "/home", "2023-10-01 10:15:00", "Redirection"),
        Row(5, 200, "/services", "2023-10-01 10:20:00", "Success"),
        Row(6, 403, "/home", "2023-10-01 10:25:00", "Client Error"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getInt(0))
    assert(results.sorted.sameElements(expectedResults.sorted))
    val expectedColumns =
      Array[String]("id", "status_code", "request_path", "timestamp", "status_category")
    assert(frame.columns.sameElements(expectedColumns))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_http_log"))
    val conditionValueSequence = Seq(
      (graterOrEqualAndLessThan("status_code", 200, 300), Literal("Success")),
      (graterOrEqualAndLessThan("status_code", 300, 400), Literal("Redirection")),
      (graterOrEqualAndLessThan("status_code", 400, 500), Literal("Client Error")),
      (
        EqualTo(
          Literal(true),
          GreaterThanOrEqual(UnresolvedAttribute("status_code"), Literal(500))),
        Literal("Server Error")))
    val elseValue = Literal("Unknown")
    val caseFunction = CaseWhen(conditionValueSequence, elseValue)
    val aliasStatusCategory = Alias(caseFunction, "status_category")()
    val evalProjectList = Seq(UnresolvedStar(None), aliasStatusCategory)
    val evalProject = Project(evalProjectList, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), evalProject)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  private def graterOrEqualAndLessThan(fieldName: String, min: Int, max: Int) = {
    val and = And(
      GreaterThanOrEqual(UnresolvedAttribute("status_code"), Literal(min)),
      LessThan(UnresolvedAttribute(fieldName), Literal(max)))
    EqualTo(Literal(true), and)
  }

  // Todo excluded fields not support yet

  ignore("test single eval expression with excluded fields") {
    val frame = sql(s"""
         | source = $testTable | eval new_field = "New Field" | fields - age
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row("Jake", "California", "USA", 2023, 4, "New Field"),
      Row("Hello", "New York", "USA", 2023, 4, "New Field"),
      Row("John", "Ontario", "Canada", 2023, 4, "New Field"),
      Row("Jane", "Quebec", "Canada", 2023, 4, "New Field"))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }
}
