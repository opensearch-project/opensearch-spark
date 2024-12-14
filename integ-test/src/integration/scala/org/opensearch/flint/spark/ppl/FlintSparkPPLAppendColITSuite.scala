/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq
import org.opensearch.sql.ppl.utils.SortUtils
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, Ascending, CaseWhen, CurrentRow, Descending, Divide, EqualTo, Expression, LessThan, Literal, Multiply, RowFrame, RowNumber, SortOrder, SpecifiedWindowFrame, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest



class FlintSparkPPLAppendColITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"

  private val ROW_NUMBER_AGGREGATION = Alias(
    WindowExpression(
      RowNumber(),
      WindowSpecDefinition(
        Nil,
        SortUtils.sortOrder(Literal("1"), false) :: Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))),
    "_row_number_")()

  private val COUNT_STAR = Alias(
    UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
    "count()")()

  private val AGE_ALIAS = Alias(UnresolvedAttribute("age"), "age")()

  private val RELATION_TEST_TABLE = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))

  private val T12_JOIN_CONDITION =
    EqualTo(UnresolvedAttribute("T1._row_number_"), UnresolvedAttribute("T2._row_number_"))

  private val T12_COLUMNS_SEQ =
    Seq(UnresolvedAttribute("T1._row_number_"), UnresolvedAttribute("T2._row_number_"))


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

  test("test AppendCol with NO transformation on main") {
    val frame = sql(s"""
                       | source = $testTable | APPENDCOL [stats count() by age]
                       | """.stripMargin)


    assert(
      frame.columns.sameElements(
        Array("name", "age", "state", "country", "year", "month", "count()", "age")))
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row("Jake", 70, "California", "USA", 2023, 4, 1, 70),
        Row("Hello", 30, "New York", "USA", 2023, 4, 1, 30),
        Row("John", 25, "Ontario", "Canada", 2023, 4, 1, 25),
        Row("Jane", 20, "Quebec", "Canada", 2023, 4, 1, 20))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    /*
      :- 'SubqueryAlias T1
      :  +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#7, *]
      :     +- 'UnresolvedRelation [relation], [], false
     */
    val t1 = SubqueryAlias(
      "T1",
      Project(Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)), RELATION_TEST_TABLE))

    /*
    +- 'SubqueryAlias T2
      +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST,
          specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#11, *]
        +- 'Aggregate ['age AS age#9], ['COUNT(*) AS count()#8, 'age AS age#10]
           +- 'UnresolvedRelation [relation], [], false
     */
    val t2 = SubqueryAlias(
      "T2",
      Project(
        Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
        Aggregate(AGE_ALIAS :: Nil, Seq(COUNT_STAR, AGE_ALIAS), RELATION_TEST_TABLE)))

    val expectedPlan = Project(
      Seq(UnresolvedStar(None)),
      DataFrameDropColumns(
        T12_COLUMNS_SEQ,
        Join(t1, t2, LeftOuter, Some(T12_JOIN_CONDITION), JoinHint.NONE)))

      // scalastyle:off
      println(logicalPlan)
      println(expectedPlan)
      // scalastyle:on

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }


  test("test AppendCol with transformation on main-search") {
    val frame = sql(s"""
                       | source = $testTable | FIELDS name, age, state | APPENDCOL [stats count() by age]
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("name", "age", "state", "count()", "age")))
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row("Jake", 70, "California", 1, 70),
        Row("Hello", 30, "New York", 1, 30),
        Row("John", 25, "Ontario", 1, 25),
        Row("Jane", 20, "Quebec", 1, 20))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    /*
     :- 'SubqueryAlias T1
     :  +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST,
               specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#11, *]
     :     +- 'Project ['name, 'age, 'state]
     :        +- 'UnresolvedRelation [relation], [], false
    */
    val t1 = SubqueryAlias(
      "T1",
      Project(
        Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
        Project(
          Seq(
            UnresolvedAttribute("name"),
            UnresolvedAttribute("age"),
            UnresolvedAttribute("state")),
          RELATION_TEST_TABLE)))

    /*
    +- 'SubqueryAlias T2
      +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST,
          specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#11, *]
        +- 'Aggregate ['age AS age#9], ['COUNT(*) AS count()#8, 'age AS age#10]
           +- 'UnresolvedRelation [relation], [], false
     */
    val t2 = SubqueryAlias(
      "T2",
      Project(
        Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
        Aggregate(AGE_ALIAS :: Nil, Seq(COUNT_STAR, AGE_ALIAS), RELATION_TEST_TABLE)))

    val expectedPlan = Project(
      Seq(UnresolvedStar(None)),
      DataFrameDropColumns(
        T12_COLUMNS_SEQ,
        Join(t1, t2, LeftOuter, Some(T12_JOIN_CONDITION), JoinHint.NONE)))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }
//
//  test("test multiple trendline sma commands") {
//    val frame = sql(s"""
//                       | source = $testTable | trendline sort + age sma(2, age) as two_points_sma sma(3, age) as three_points_sma | fields name, age, two_points_sma, three_points_sma
//                       | """.stripMargin)
//
//    assert(frame.columns.sameElements(Array("name", "age", "two_points_sma", "three_points_sma")))
//    // Retrieve the results
//    val results: Array[Row] = frame.collect()
//    val expectedResults: Array[Row] =
//      Array(
//        Row("Jane", 20, null, null),
//        Row("John", 25, 22.5, null),
//        Row("Hello", 30, 27.5, 25.0),
//        Row("Jake", 70, 50.0, 41.666666666666664))
//    // Compare the results
//    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
//    assert(results.sorted.sameElements(expectedResults.sorted))
//
//    // Retrieve the logical plan
//    val logicalPlan: LogicalPlan = frame.queryExecution.logical
//    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
//    val nameField = UnresolvedAttribute("name")
//    val ageField = UnresolvedAttribute("age")
//    val ageTwoPointsSmaField = UnresolvedAttribute("two_points_sma")
//    val ageThreePointsSmaField = UnresolvedAttribute("three_points_sma")
//    val sort = Sort(Seq(SortOrder(ageField, Ascending)), global = true, table)
//    val twoPointsCountWindow = new WindowExpression(
//      UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
//      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-1), CurrentRow)))
//    val twoPointsSmaWindow = WindowExpression(
//      UnresolvedFunction("AVG", Seq(ageField), isDistinct = false),
//      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-1), CurrentRow)))
//    val threePointsCountWindow = new WindowExpression(
//      UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
//      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-2), CurrentRow)))
//    val threePointsSmaWindow = WindowExpression(
//      UnresolvedFunction("AVG", Seq(ageField), isDistinct = false),
//      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-2), CurrentRow)))
//    val twoPointsCaseWhen = CaseWhen(
//      Seq((LessThan(twoPointsCountWindow, Literal(2)), Literal(null))),
//      twoPointsSmaWindow)
//    val threePointsCaseWhen = CaseWhen(
//      Seq((LessThan(threePointsCountWindow, Literal(3)), Literal(null))),
//      threePointsSmaWindow)
//    val trendlineProjectList = Seq(
//      UnresolvedStar(None),
//      Alias(twoPointsCaseWhen, "two_points_sma")(),
//      Alias(threePointsCaseWhen, "three_points_sma")())
//    val expectedPlan = Project(
//      Seq(nameField, ageField, ageTwoPointsSmaField, ageThreePointsSmaField),
//      Project(trendlineProjectList, sort))
//    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
//  }
//
//  test("test trendline sma command on evaluated column") {
//    val frame = sql(s"""
//                       | source = $testTable | eval doubled_age = age * 2 | trendline sort + age sma(2, doubled_age) as doubled_age_sma | fields name, doubled_age, doubled_age_sma
//                       | """.stripMargin)
//
//    assert(frame.columns.sameElements(Array("name", "doubled_age", "doubled_age_sma")))
//    // Retrieve the results
//    val results: Array[Row] = frame.collect()
//    val expectedResults: Array[Row] =
//      Array(
//        Row("Jane", 40, null),
//        Row("John", 50, 45.0),
//        Row("Hello", 60, 55.0),
//        Row("Jake", 140, 100.0))
//    // Compare the results
//    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
//    assert(results.sorted.sameElements(expectedResults.sorted))
//
//    // Retrieve the logical plan
//    val logicalPlan: LogicalPlan = frame.queryExecution.logical
//    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
//    val nameField = UnresolvedAttribute("name")
//    val ageField = UnresolvedAttribute("age")
//    val doubledAgeField = UnresolvedAttribute("doubled_age")
//    val doubledAgeSmaField = UnresolvedAttribute("doubled_age_sma")
//    val evalProject = Project(
//      Seq(
//        UnresolvedStar(None),
//        Alias(
//          UnresolvedFunction("*", Seq(ageField, Literal(2)), isDistinct = false),
//          "doubled_age")()),
//      table)
//    val sort = Sort(Seq(SortOrder(ageField, Ascending)), global = true, evalProject)
//    val countWindow = new WindowExpression(
//      UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
//      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-1), CurrentRow)))
//    val doubleAgeSmaWindow = WindowExpression(
//      UnresolvedFunction("AVG", Seq(doubledAgeField), isDistinct = false),
//      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-1), CurrentRow)))
//    val caseWhen =
//      CaseWhen(Seq((LessThan(countWindow, Literal(2)), Literal(null))), doubleAgeSmaWindow)
//    val trendlineProjectList =
//      Seq(UnresolvedStar(None), Alias(caseWhen, "doubled_age_sma")())
//    val expectedPlan = Project(
//      Seq(nameField, doubledAgeField, doubledAgeSmaField),
//      Project(trendlineProjectList, sort))
//    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
//  }
//
//  test("test trendline sma command chaining") {
//    val frame = sql(s"""
//                       | source = $testTable | eval age_1 = age, age_2 = age | trendline sort - age_1 sma(3, age_1) | trendline sort + age_2 sma(3, age_2)
//                       | """.stripMargin)
//
//    assert(
//      frame.columns.sameElements(
//        Array(
//          "name",
//          "age",
//          "state",
//          "country",
//          "year",
//          "month",
//          "age_1",
//          "age_2",
//          "age_1_trendline",
//          "age_2_trendline")))
//    // Retrieve the results
//    val results: Array[Row] = frame.collect()
//    val expectedResults: Array[Row] =
//      Array(
//        Row("Hello", 30, "New York", "USA", 2023, 4, 30, 30, null, 25.0),
//        Row("Jake", 70, "California", "USA", 2023, 4, 70, 70, null, 41.666666666666664),
//        Row("Jane", 20, "Quebec", "Canada", 2023, 4, 20, 20, 25.0, null),
//        Row("John", 25, "Ontario", "Canada", 2023, 4, 25, 25, 41.666666666666664, null))
//    // Compare the results
//    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
//    assert(results.sorted.sameElements(expectedResults.sorted))
//  }
//
//  test("test trendline wma command with sort field and without alias") {
//    val frame = sql(s"""
//                       | source = $testTable | trendline sort + age wma(3, age)
//                       | """.stripMargin)
//
//    // Compare the headers
//    assert(
//      frame.columns.sameElements(
//        Array("name", "age", "state", "country", "year", "month", "age_trendline")))
//    // Retrieve the results
//    val results: Array[Row] = frame.collect()
//    val expectedResults: Array[Row] =
//      Array(
//        Row("Jane", 20, "Quebec", "Canada", 2023, 4, null),
//        Row("John", 25, "Ontario", "Canada", 2023, 4, null),
//        Row("Hello", 30, "New York", "USA", 2023, 4, 26.666666666666668),
//        Row("Jake", 70, "California", "USA", 2023, 4, 49.166666666666664))
//
//    // Compare the results
//    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
//    assert(results.sorted.sameElements(expectedResults.sorted))
//
//    // Compare the logical plans
//    val logicalPlan: LogicalPlan = frame.queryExecution.logical
//    val dividend = Add(
//      Add(
//        getNthValueAggregation("age", "age", 1, -2),
//        getNthValueAggregation("age", "age", 2, -2)),
//      getNthValueAggregation("age", "age", 3, -2))
//    val wmaExpression = Divide(dividend, Literal(6))
//    val trendlineProjectList = Seq(UnresolvedStar(None), Alias(wmaExpression, "age_trendline")())
//    val unresolvedRelation = UnresolvedRelation(testTable.split("\\.").toSeq)
//    val sortedTable = Sort(
//      Seq(SortOrder(UnresolvedAttribute("age"), Ascending)),
//      global = true,
//      unresolvedRelation)
//    val expectedPlan =
//      Project(Seq(UnresolvedStar(None)), Project(trendlineProjectList, sortedTable))
//
//    /**
//     * Expected logical plan: 'Project [*] +- 'Project [*, ((( ('nth_value('age, 1)
//     * windowspecdefinition('age ASC NULLS FIRST, specifiedwindowframe(RowFrame, -2,
//     * currentrow$())) * 1) + ('nth_value('age, 2) windowspecdefinition('age ASC NULLS FIRST,
//     * specifiedwindowframe(RowFrame, -2, currentrow$())) * 2)) + ('nth_value('age, 3)
//     * windowspecdefinition('age ASC NULLS FIRST, specifiedwindowframe(RowFrame, -2,
//     * currentrow$())) * 3)) / 6) AS age_trendline#185] +- 'Sort ['age ASC NULLS FIRST], true +-
//     * 'UnresolvedRelation [spark_catalog, default, flint_ppl_test], [], false
//     */
//    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
//  }
//
//  test("test trendline wma command with sort field and with alias") {
//    val frame = sql(s"""
//                       | source = $testTable | trendline sort + age wma(3, age) as trendline_alias
//                       | """.stripMargin)
//
//    // Compare the headers
//    assert(
//      frame.columns.sameElements(
//        Array("name", "age", "state", "country", "year", "month", "trendline_alias")))
//    // Retrieve the results
//    val results: Array[Row] = frame.collect()
//    val expectedResults: Array[Row] =
//      Array(
//        Row("Jane", 20, "Quebec", "Canada", 2023, 4, null),
//        Row("John", 25, "Ontario", "Canada", 2023, 4, null),
//        Row("Hello", 30, "New York", "USA", 2023, 4, 26.666666666666668),
//        Row("Jake", 70, "California", "USA", 2023, 4, 49.166666666666664))
//
//    // Compare the results
//    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
//    assert(results.sorted.sameElements(expectedResults.sorted))
//
//    // Compare the logical plans
//    val logicalPlan: LogicalPlan = frame.queryExecution.logical
//    val dividend = Add(
//      Add(
//        getNthValueAggregation("age", "age", 1, -2),
//        getNthValueAggregation("age", "age", 2, -2)),
//      getNthValueAggregation("age", "age", 3, -2))
//    val wmaExpression = Divide(dividend, Literal(6))
//    val trendlineProjectList =
//      Seq(UnresolvedStar(None), Alias(wmaExpression, "trendline_alias")())
//    val unresolvedRelation = UnresolvedRelation(testTable.split("\\.").toSeq)
//    val sortedTable = Sort(
//      Seq(SortOrder(UnresolvedAttribute("age"), Ascending)),
//      global = true,
//      unresolvedRelation)
//    val expectedPlan =
//      Project(Seq(UnresolvedStar(None)), Project(trendlineProjectList, sortedTable))
//
//    /**
//     * 'Project [*] +- 'Project [*, ((( ('nth_value('age, 1) windowspecdefinition('age ASC NULLS
//     * FIRST, specifiedwindowframe(RowFrame, -2, currentrow$())) * 1) + ('nth_value('age, 2)
//     * windowspecdefinition('age ASC NULLS FIRST, specifiedwindowframe(RowFrame, -2,
//     * currentrow$())) * 2)) + ('nth_value('age, 3) windowspecdefinition('age ASC NULLS FIRST,
//     * specifiedwindowframe(RowFrame, -2, currentrow$())) * 3)) / 6) AS trendline_alias#185] +-
//     * 'Sort ['age ASC NULLS FIRST], true +- 'UnresolvedRelation [spark_catalog, default,
//     * flint_ppl_test], [], false
//     */
//    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
//  }
//
//  test("test multiple trendline wma commands") {
//    val frame = sql(s"""
//                       | source = $testTable | trendline sort + age wma(2, age) as two_points_wma wma(3, age) as three_points_wma
//                       | """.stripMargin)
//
//    // Compare the headers
//    assert(
//      frame.columns.sameElements(
//        Array(
//          "name",
//          "age",
//          "state",
//          "country",
//          "year",
//          "month",
//          "two_points_wma",
//          "three_points_wma")))
//    // Retrieve the results
//    val results: Array[Row] = frame.collect()
//    val expectedResults: Array[Row] =
//      Array(
//        Row("Jane", 20, "Quebec", "Canada", 2023, 4, null, null),
//        Row("John", 25, "Ontario", "Canada", 2023, 4, 23.333333333333332, null),
//        Row("Hello", 30, "New York", "USA", 2023, 4, 28.333333333333332, 26.666666666666668),
//        Row("Jake", 70, "California", "USA", 2023, 4, 56.666666666666664, 49.166666666666664))
//
//    // Compare the results
//    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
//    assert(results.sorted.sameElements(expectedResults.sorted))
//
//    // Compare the logical plans
//    val logicalPlan: LogicalPlan = frame.queryExecution.logical
//
//    val dividendTwo = Add(
//      getNthValueAggregation("age", "age", 1, -1),
//      getNthValueAggregation("age", "age", 2, -1))
//    val twoPointsExpression = Divide(dividendTwo, Literal(3))
//
//    val dividend = Add(
//      Add(
//        getNthValueAggregation("age", "age", 1, -2),
//        getNthValueAggregation("age", "age", 2, -2)),
//      getNthValueAggregation("age", "age", 3, -2))
//    val threePointsExpression = Divide(dividend, Literal(6))
//
//    val trendlineProjectList = Seq(
//      UnresolvedStar(None),
//      Alias(twoPointsExpression, "two_points_wma")(),
//      Alias(threePointsExpression, "three_points_wma")())
//    val unresolvedRelation = UnresolvedRelation(testTable.split("\\.").toSeq)
//    val sortedTable = Sort(
//      Seq(SortOrder(UnresolvedAttribute("age"), Ascending)),
//      global = true,
//      unresolvedRelation)
//    val expectedPlan =
//      Project(Seq(UnresolvedStar(None)), Project(trendlineProjectList, sortedTable))
//
//    /**
//     * 'Project [*] +- 'Project [*, (( ('nth_value('age, 1) windowspecdefinition('age ASC NULLS
//     * FIRST, specifiedwindowframe(RowFrame, -1, currentrow$())) * 1) + ('nth_value('age, 2)
//     * windowspecdefinition('age ASC NULLS FIRST, specifiedwindowframe(RowFrame, -1,
//     * currentrow$())) * 2)) / 3) AS two_points_wma#247,
//     *
//     * ((( ('nth_value('age, 1) windowspecdefinition('age ASC NULLS FIRST,
//     * specifiedwindowframe(RowFrame, -2, currentrow$())) * 1) + ('nth_value('age, 2)
//     * windowspecdefinition('age ASC NULLS FIRST, specifiedwindowframe(RowFrame, -2,
//     * currentrow$())) * 2)) + ('nth_value('age, 3) windowspecdefinition('age ASC NULLS FIRST,
//     * specifiedwindowframe(RowFrame, -2, currentrow$())) * 3)) / 6) AS three_points_wma#248] +-
//     * 'Sort ['age ASC NULLS FIRST], true +- 'UnresolvedRelation [spark_catalog, default,
//     * flint_ppl_test], [], false
//     */
//    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
//  }
//
//  test("test trendline wma command on evaluated column") {
//    val frame = sql(s"""
//                       | source = $testTable | eval doubled_age = age * 2 | trendline sort + age wma(2, doubled_age) as doubled_age_wma | fields name, doubled_age, doubled_age_wma
//                       | """.stripMargin)
//
//    // Compare the headers
//    assert(frame.columns.sameElements(Array("name", "doubled_age", "doubled_age_wma")))
//    // Retrieve the results
//    val results: Array[Row] = frame.collect()
//    val expectedResults: Array[Row] =
//      Array(
//        Row("Jane", 40, null),
//        Row("John", 50, 46.666666666666664),
//        Row("Hello", 60, 56.666666666666664),
//        Row("Jake", 140, 113.33333333333333))
//
//    // Compare the results
//    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
//    assert(results.sorted.sameElements(expectedResults.sorted))
//
//    // Compare the logical plans
//    val logicalPlan: LogicalPlan = frame.queryExecution.logical
//    val dividend = Add(
//      getNthValueAggregation("doubled_age", "age", 1, -1),
//      getNthValueAggregation("doubled_age", "age", 2, -1))
//    val wmaExpression = Divide(dividend, Literal(3))
//    val trendlineProjectList =
//      Seq(UnresolvedStar(None), Alias(wmaExpression, "doubled_age_wma")())
//    val unresolvedRelation = UnresolvedRelation(testTable.split("\\.").toSeq)
//    val doubledAged = Alias(
//      UnresolvedFunction(
//        seq("*"),
//        seq(UnresolvedAttribute("age"), Literal(2)),
//        isDistinct = false),
//      "doubled_age")()
//    val doubleAgeProject = Project(seq(UnresolvedStar(None), doubledAged), unresolvedRelation)
//    val sortedTable =
//      Sort(Seq(SortOrder(UnresolvedAttribute("age"), Ascending)), global = true, doubleAgeProject)
//    val expectedPlan = Project(
//      Seq(
//        UnresolvedAttribute("name"),
//        UnresolvedAttribute("doubled_age"),
//        UnresolvedAttribute("doubled_age_wma")),
//      Project(trendlineProjectList, sortedTable))
//
//    /**
//     * 'Project ['name, 'doubled_age, 'doubled_age_wma] +- 'Project [*, ((
//     * ('nth_value('doubled_age, 1) windowspecdefinition('age ASC NULLS FIRST,
//     * specifiedwindowframe(RowFrame, -1, currentrow$())) * 1) + ('nth_value('doubled_age, 2)
//     * windowspecdefinition('age ASC NULLS FIRST, specifiedwindowframe(RowFrame, -1,
//     * currentrow$())) * 2)) / 3) AS doubled_age_wma#288] +- 'Sort ['age ASC NULLS FIRST], true +-
//     * 'Project [*, '`*`('age, 2) AS doubled_age#287] +- 'UnresolvedRelation [spark_catalog,
//     * default, flint_ppl_test], [], false
//     */
//    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
//
//  }
//
//  test("test invalid wma command with negative dataPoint value") {
//    val exception = intercept[ParseException](sql(s"""
//           | source = $testTable | trendline sort + age wma(-3, age)
//           | """.stripMargin))
//    assert(exception.getMessage contains "[PARSE_SYNTAX_ERROR] Syntax error")
//  }

  private def getNthValueAggregation(
      dataField: String,
      sortField: String,
      lookBackPos: Int,
      lookBackRange: Int): Expression = {
    Multiply(
      WindowExpression(
        UnresolvedFunction(
          "nth_value",
          Seq(UnresolvedAttribute(dataField), Literal(lookBackPos)),
          isDistinct = false),
        WindowSpecDefinition(
          Seq(),
          seq(SortUtils.sortOrder(UnresolvedAttribute(sortField), true)),
          SpecifiedWindowFrame(RowFrame, Literal(lookBackRange), CurrentRow))),
      Literal(lookBackPos))
  }

}
