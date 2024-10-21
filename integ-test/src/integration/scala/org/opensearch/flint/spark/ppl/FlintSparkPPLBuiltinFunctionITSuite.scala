/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import java.sql.{Date, Time, Timestamp}

import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, CaseWhen, EqualTo, GreaterThan, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, GlobalLimit, LocalLimit, LogicalPlan, Project}
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.DoubleType

class FlintSparkPPLBuiltinFunctionITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"
  private val testNullTable = "spark_catalog.default.flint_ppl_test_null"
  private val testTextSizeTable = "spark_catalog.default.flint_ppl_text_size"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createPartitionedStateCountryTable(testTable)
    createNullableStateCountryTable(testNullTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test string functions - concat") {
    val frame = sql(s"""
       | source = $testTable name=concat('He', 'llo') | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Hello", 30))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("name"),
      UnresolvedFunction("concat", seq(Literal("He"), Literal("llo")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test string functions - concat with field") {
    val frame = sql(s"""
       | source = $testTable name=concat('Hello', state) | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array.empty
    assert(results.sameElements(expectedResults))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("name"),
      UnresolvedFunction(
        "concat",
        seq(Literal("Hello"), UnresolvedAttribute("state")),
        isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test string functions - length") {
    val frame = sql(s"""
       | source = $testTable |where length(name) = 5 | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Hello", 30))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedFunction("length", seq(UnresolvedAttribute("name")), isDistinct = false),
      Literal(5))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test function name should be insensitive") {
    val frame = sql(s"""
       | source = $testTable |where leNgTh(name) = 5 | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Hello", 30))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedFunction("length", seq(UnresolvedAttribute("name")), isDistinct = false),
      Literal(5))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test string functions - lower") {
    val frame = sql(s"""
       | source = $testTable |where lower(name) = "hello" | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Hello", 30))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedFunction("lower", seq(UnresolvedAttribute("name")), isDistinct = false),
      Literal("hello"))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test string functions - upper") {
    val frame = sql(s"""
       | source = $testTable |where upper(name) = upper("hello") | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Hello", 30))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedFunction("upper", seq(UnresolvedAttribute("name")), isDistinct = false),
      UnresolvedFunction("upper", seq(Literal("hello")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test string functions - substring") {
    val frame = sql(s"""
       | source = $testTable |where substring(name, 2, 2) = "el" | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Hello", 30))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedFunction(
        "substring",
        seq(UnresolvedAttribute("name"), Literal(2), Literal(2)),
        isDistinct = false),
      Literal("el"))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test string functions - like") {
    val frame = sql(s"""
       | source = $testTable | where like(name, '_ello%') | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Hello", 30))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val likeFunction = UnresolvedFunction(
      "like",
      seq(UnresolvedAttribute("name"), Literal("_ello%")),
      isDistinct = false)

    val filterPlan = Filter(likeFunction, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test string functions - replace") {
    val frame = sql(s"""
       | source = $testTable |where replace(name, 'o', ' ') = "Hell " | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Hello", 30))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedFunction(
        "replace",
        seq(UnresolvedAttribute("name"), Literal("o"), Literal(" ")),
        isDistinct = false),
      Literal("Hell "))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test string functions - replace and trim") {
    val frame = sql(s"""
       | source = $testTable |where trim(replace(name, 'o', ' ')) = "Hell" | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Hello", 30))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedFunction(
        "trim",
        seq(
          UnresolvedFunction(
            "replace",
            seq(UnresolvedAttribute("name"), Literal("o"), Literal(" ")),
            isDistinct = false)),
        isDistinct = false),
      Literal("Hell"))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test string functions - isempty eval") {
    val frame = sql(s"""
                       | source = $testNullTable | head 1 | eval a = isempty('full'), b = isempty(''), c = isempty(' ') | fields a, b, c
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(false, true, true))
    assert(results.sameElements(expectedResults))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_null"))
    val localLimit = LocalLimit(Literal(1), table)
    val globalLimit = GlobalLimit(Literal(1), localLimit)

    //    val projectList = Seq(UnresolvedStar(None))

    val caseOne = CaseWhen(
      Seq(
        (
          EqualTo(
            UnresolvedFunction(
              "length",
              Seq(UnresolvedFunction("trim", Seq(Literal("full")), isDistinct = false)),
              isDistinct = false),
            Literal(0)),
          Literal(true))),
      Literal(false))
    val aliasOne = Alias(caseOne, "a")()

    val caseTwo = CaseWhen(
      Seq(
        (
          EqualTo(
            UnresolvedFunction(
              "length",
              Seq(UnresolvedFunction("trim", Seq(Literal("")), isDistinct = false)),
              isDistinct = false),
            Literal(0)),
          Literal(true))),
      Literal(false))
    val aliasTwo = Alias(caseTwo, "b")()

    val caseThree = CaseWhen(
      Seq(
        (
          EqualTo(
            UnresolvedFunction(
              "length",
              Seq(UnresolvedFunction("trim", Seq(Literal(" ")), isDistinct = false)),
              isDistinct = false),
            Literal(0)),
          Literal(true))),
      Literal(false))
    val aliasThree = Alias(caseThree, "c")()

    val projectList = Seq(UnresolvedStar(None), aliasOne, aliasTwo, aliasThree)
    val innerProject = Project(projectList, globalLimit)

    val expectedPlan = Project(
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c")),
      innerProject)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test string functions - isempty where") {
    val frame = sql(s"""
                       | source = $testNullTable | where isempty('I am not empty');
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    assert(results.length == 0)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_null"))
    val caseIsEmpty = CaseWhen(
      Seq(
        (
          EqualTo(
            UnresolvedFunction(
              "length",
              Seq(UnresolvedFunction("trim", Seq(Literal("I am not empty")), isDistinct = false)),
              isDistinct = false),
            Literal(0)),
          Literal(true))),
      Literal(false))
    val filterPlan = Filter(caseIsEmpty, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test math functions - abs") {
    val frame = sql(s"""
       | source = $testTable |where age = abs(-30) | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Hello", 30))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("age"),
      UnresolvedFunction("abs", seq(Literal(-30)), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test string functions - isblank eval") {
    val frame = sql(s"""
                       | source = $testNullTable | head 1 | eval a = isblank('full'), b = isblank(''), c = isblank(' ') | fields a, b, c
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(false, true, true))
    assert(results.sameElements(expectedResults))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_null"))
    val localLimit = LocalLimit(Literal(1), table)
    val globalLimit = GlobalLimit(Literal(1), localLimit)

    //    val projectList = Seq(UnresolvedStar(None))

    val caseOne = CaseWhen(
      Seq(
        (
          EqualTo(
            UnresolvedFunction(
              "length",
              Seq(UnresolvedFunction("trim", Seq(Literal("full")), isDistinct = false)),
              isDistinct = false),
            Literal(0)),
          Literal(true))),
      Literal(false))
    val aliasOne = Alias(caseOne, "a")()

    val caseTwo = CaseWhen(
      Seq(
        (
          EqualTo(
            UnresolvedFunction(
              "length",
              Seq(UnresolvedFunction("trim", Seq(Literal("")), isDistinct = false)),
              isDistinct = false),
            Literal(0)),
          Literal(true))),
      Literal(false))
    val aliasTwo = Alias(caseTwo, "b")()

    val caseThree = CaseWhen(
      Seq(
        (
          EqualTo(
            UnresolvedFunction(
              "length",
              Seq(UnresolvedFunction("trim", Seq(Literal(" ")), isDistinct = false)),
              isDistinct = false),
            Literal(0)),
          Literal(true))),
      Literal(false))
    val aliasThree = Alias(caseThree, "c")()

    val projectList = Seq(UnresolvedStar(None), aliasOne, aliasTwo, aliasThree)
    val innerProject = Project(projectList, globalLimit)

    val expectedPlan = Project(
      Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c")),
      innerProject)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test string functions - isblank where") {
    val frame = sql(s"""
                       | source = $testNullTable | where isblank('I am not blank');
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    assert(results.length == 0)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test_null"))
    val caseIsEmpty = CaseWhen(
      Seq(
        (
          EqualTo(
            UnresolvedFunction(
              "length",
              Seq(UnresolvedFunction("trim", Seq(Literal("I am not blank")), isDistinct = false)),
              isDistinct = false),
            Literal(0)),
          Literal(true))),
      Literal(false))
    val filterPlan = Filter(caseIsEmpty, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test math functions - abs with field") {
    val frame = sql(s"""
       | source = $testTable |where abs(age) = 30 | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Hello", 30))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedFunction("abs", seq(UnresolvedAttribute("age")), isDistinct = false),
      Literal(30))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test math functions - ceil") {
    val frame = sql(s"""
       | source = $testTable |where age = ceil(29.7) | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Hello", 30))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("age"),
      UnresolvedFunction("ceil", seq(Literal(29.7d, DoubleType)), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test math functions - floor") {
    val frame = sql(s"""
       | source = $testTable |where age = floor(30.4) | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Hello", 30))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("age"),
      UnresolvedFunction("floor", seq(Literal(30.4d, DoubleType)), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test math functions - ln") {
    val frame = sql(s"""
       | source = $testTable |where ln(age) > 4 | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Jake", 70))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = GreaterThan(
      UnresolvedFunction("ln", seq(UnresolvedAttribute("age")), isDistinct = false),
      Literal(4))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test math functions - mod") {
    val frame = sql(s"""
       | source = $testTable |where mod(age, 10) = 0 | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Jake", 70), Row("Hello", 30), Row("Jane", 20))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedFunction("mod", seq(UnresolvedAttribute("age"), Literal(10)), isDistinct = false),
      Literal(0))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test math functions - pow and sqrt") {
    val frame = sql(s"""
       | source = $testTable |where sqrt(pow(age, 2)) = 30.0 | fields name, age
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Hello", 30))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedFunction(
        "sqrt",
        seq(
          UnresolvedFunction(
            "pow",
            seq(UnresolvedAttribute("age"), Literal(2)),
            isDistinct = false)),
        isDistinct = false),
      Literal(30.0))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test arithmetic operators (+ - * / %)") {
    val frame = sql(s"""
       | source = $testTable | where (sqrt(pow(age, 2)) + sqrt(pow(age, 2)) / 1 - sqrt(pow(age, 2)) * 1) % 25.0 = 0 | fields name, age
       | """.stripMargin) // equals age + age - age

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("John", 25))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test boolean operators (= != < <= > >=)") {
    val frame = sql(s"""
       | source = $testTable | eval a = age = 30, b = age != 70, c = 30 < age, d = 30 <= age, e = 30 > age, f = 30 >= age | fields age, a, b, c, d, e, f
       | """.stripMargin) // equals age + age - age

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(70, false, false, true, true, false, false),
      Row(30, true, true, false, true, false, true),
      Row(25, false, true, false, false, true, true),
      Row(20, false, true, false, false, true, true))
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Integer](_.getAs[Integer](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test boolean condition functions - isnull isnotnull ifnull nullif ispresent") {
    val frameIsNull = sql(s"""
       | source = $testNullTable | where isnull(name)  | fields age
       | """.stripMargin)

    val results1: Array[Row] = frameIsNull.collect()
    val expectedResults1: Array[Row] = Array(Row(10))
    assert(results1.sameElements(expectedResults1))

    val frameIsNotNull = sql(s"""
       | source = $testNullTable | where isnotnull(name)  | fields name
       | """.stripMargin)

    val results2: Array[Row] = frameIsNotNull.collect()
    val expectedResults2: Array[Row] = Array(Row("John"), Row("Jane"), Row("Jake"), Row("Hello"))
    assert(results2.sameElements(expectedResults2))

    val frameIfNull = sql(s"""
       | source = $testNullTable | eval new_name = ifnull(name, "Unknown")  | fields new_name, age
       | """.stripMargin)

    val results3: Array[Row] = frameIfNull.collect()
    val expectedResults3: Array[Row] = Array(
      Row("John", 25),
      Row("Jane", 20),
      Row("Unknown", 10),
      Row("Jake", 70),
      Row("Hello", 30))
    assert(results3.sameElements(expectedResults3))

    val frameNullIf = sql(s"""
       | source = $testNullTable | eval new_age = nullif(age, 20)  | fields name, new_age
       | """.stripMargin)

    val results4: Array[Row] = frameNullIf.collect()
    val expectedResults4: Array[Row] =
      Array(Row("John", 25), Row("Jane", null), Row(null, 10), Row("Jake", 70), Row("Hello", 30))
    assert(results4.sameElements(expectedResults4))

    val frameIsPresent = sql(s"""
                                | source = $testNullTable | where ispresent(name)  | fields name
                                | """.stripMargin)

    val results5: Array[Row] = frameIsPresent.collect()
    val expectedResults5: Array[Row] = Array(Row("John"), Row("Jane"), Row("Jake"), Row("Hello"))
    assert(results5.sameElements(expectedResults5))

    val frameEvalIsPresent = sql(s"""
                             | source = $testNullTable | eval hasName = ispresent(name)  | fields name, hasName
                             | """.stripMargin)

    val results6: Array[Row] = frameEvalIsPresent.collect()
    val expectedResults6: Array[Row] = Array(
      Row("John", true),
      Row("Jane", true),
      Row(null, false),
      Row("Jake", true),
      Row("Hello", true))
    assert(results6.sameElements(expectedResults6))
  }

  test("test typeof function") {
    val frame = sql(s"""
       | source = $testNullTable | eval tdate = typeof(DATE('2008-04-14')), tint = typeof(1), tnow = typeof(now()), tcol = typeof(age) | fields tdate, tint, tnow, tcol | head 1
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("date", "int", "timestamp", "int"))
    assert(results.sameElements(expectedResults))
  }

  test("test the builtin functions which required additional name mapping") {
    val frame = sql(s"""
     | source = $testNullTable
     | | eval a = DAY_OF_WEEK(DATE('2020-08-26'))
     | | eval b = DAY_OF_MONTH(DATE('2020-08-26'))
     | | eval c = DAY_OF_YEAR(DATE('2020-08-26'))
     | | eval d = WEEK_OF_YEAR(DATE('2020-08-26'))
     | | eval e = WEEK(DATE('2020-08-26'))
     | | eval f = MONTH_OF_YEAR(DATE('2020-08-26'))
     | | eval g = HOUR_OF_DAY(DATE('2020-08-26'))
     | | eval h = MINUTE_OF_HOUR(DATE('2020-08-26'))
     | | eval i = SECOND_OF_MINUTE(DATE('2020-08-26'))
     | | eval j = SUBDATE(DATE('2020-08-26'), 1)
     | | eval k = ADDDATE(DATE('2020-08-26'), 1)
     | | eval l = DATEDIFF(TIMESTAMP('2000-01-02 00:00:00'), TIMESTAMP('2000-01-01 23:59:59'))
     | | eval m = DATEDIFF(ADDDATE(LOCALTIME(), 1), LOCALTIME())
     | | fields a, b, c, d, e, f, g, h, i, j, k, l, m
     | | head 1
     | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = {
      Array(
        Row(
          4,
          26,
          239,
          35,
          35,
          8,
          0,
          0,
          0,
          Date.valueOf("2020-08-25"),
          Date.valueOf("2020-08-27"),
          1,
          1))
    }
    assert(results.sameElements(expectedResults))
  }

  test("not all arguments could work in builtin functions") {
    intercept[AnalysisException](sql(s"""
             | source = $testTable | eval a = WEEK(DATE('2008-02-20'), 1)
             | """.stripMargin))
    intercept[AnalysisException](sql(s"""
             | source = $testTable | eval a = SUBDATE(DATE('2020-08-26'), INTERVAL 31 DAY)
             | """.stripMargin))
    intercept[AnalysisException](sql(s"""
             | source = $testTable | eval a = ADDDATE(DATE('2020-08-26'), INTERVAL 1 HOUR)
             | """.stripMargin))
  }

  test("test coalesce function") {
    val frame = sql(s"""
                       | source = $testNullTable | where age = 10 | eval result=coalesce(name, state, country) | fields result
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("Canada"))
    assert(results.sameElements(expectedResults))
  }

  test("test coalesce function nulls only") {
    val frame = sql(s"""
                       | source = $testNullTable | where age = 10 | eval result=coalesce(name, state) | fields result
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(null))
    assert(results.sameElements(expectedResults))
  }

  test("test coalesce function where") {
    val frame = sql(s"""
                       | source = $testNullTable | where isnull(coalesce(name, state))
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(null, 10, null, "Canada"))
    assert(results.sameElements(expectedResults))
  }

  test("test cryptographic hash functions - md5") {
    val frame = sql(s"""
                       | source = $testTable digest=md5('Spark') | fields digest
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("8cde774d6f7333752ed72cacddb05126"))
    assert(results.sameElements(expectedResults))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("digest"),
      UnresolvedFunction(
        "md5",
        seq(Literal("Spark")),
        isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("digest"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test cryptographic hash functions - sha1") {
    val frame = sql(s"""
                       | source = $testTable digest=sha1('Spark') | fields digest
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c"))
    assert(results.sameElements(expectedResults))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("digest"),
      UnresolvedFunction(
        "sha1",
        seq(Literal("Spark")),
        isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("digest"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test cryptographic hash functions - sha2") {
    val frame = sql(s"""
                       | source = $testTable digest=sha2('Spark',256) | fields digest
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row("529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b"))
    assert(results.sameElements(expectedResults))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("digest"),
      UnresolvedFunction(
        "sha2",
        seq(Literal("Spark"), Literal(256)),
        isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("digest"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  // Todo
  // +---------------------------------------+
  // | Below tests are not supported (cast)  |
  // +---------------------------------------+
  ignore("test cast to string") {
    val frame = sql(s"""
       | source = $testNullTable | eval cbool = CAST(true as string), cint = CAST(1 as string), cdate = CAST(CAST('2012-08-07' as date) as string) | fields cbool, cint, cdate
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(true, 1, "2012-08-07"))
    assert(results.sameElements(expectedResults))
  }

  ignore("test cast to number") {
    val frame = sql(s"""
       | source = $testNullTable | eval cbool = CAST(true as int), cstring = CAST('1' as int) | fields cbool, cstring
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(1, 1))
    assert(results.sameElements(expectedResults))
  }

  ignore("test cast to date") {
    val frame = sql(s"""
       | source = $testNullTable | eval cdate = CAST('2012-08-07' as date), ctime = CAST('01:01:01' as time), ctimestamp = CAST('2012-08-07 01:01:01' as timestamp) | fields cdate, ctime, ctimestamp
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(
        Date.valueOf("2012-08-07"),
        Time.valueOf("01:01:01"),
        Timestamp.valueOf("2012-08-07 01:01:01")))
    assert(results.sameElements(expectedResults))
  }

  ignore("test can be chained") {
    val frame = sql(s"""
       | source = $testNullTable | eval cbool = CAST(CAST(true as string) as boolean) | fields cbool
       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(Row(true))
    assert(results.sameElements(expectedResults))
  }
}
