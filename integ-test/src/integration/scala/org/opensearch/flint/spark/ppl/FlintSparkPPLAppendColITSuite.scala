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

  private val RELATION_TEST_TABLE = UnresolvedRelation(
    Seq("spark_catalog", "default", "flint_ppl_test"))

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

  /**
   * The baseline test-case to make sure APPENDCOL( ) function works, when no transformation
   * present on the main search, after the search command.
   */
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

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  /**
   * To simulate the use-case when user attempt to attach an APPENDCOL command on a well
   * established main search.
   */
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

  /**
   * To simulate the situation when multiple PPL commands being applied on the sub-search.
   */
  test("test AppendCol with chained sub-search") {
    val frame = sql(s"""
                       | source = $testTable | FIELDS name, age, state | APPENDCOL [ stats count() by age | eval m = 1 | FIELDS -m ]
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
     :     +- 'Project ['age, 'dept, 'salary]
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
     +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#432, *]
        +- 'DataFrameDropColumns ['m]
           +- 'Project [*, 1 AS m#430]
              +- 'Aggregate ['age AS age#429], ['COUNT(*) AS count()#428, 'age AS age#429]
                 +- 'UnresolvedRelation [flint_ppl_test], [], false
     */
    val t2 = SubqueryAlias(
      "T2",
      Project(
        Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
        DataFrameDropColumns(
          Seq(UnresolvedAttribute("m")),
          Project(
            Seq(UnresolvedStar(None), Alias(Literal(1), "m")()),
            Aggregate(AGE_ALIAS :: Nil, Seq(COUNT_STAR, AGE_ALIAS), RELATION_TEST_TABLE)))))

    val expectedPlan = Project(
      Seq(UnresolvedStar(None)),
      DataFrameDropColumns(
        T12_COLUMNS_SEQ,
        Join(t1, t2, LeftOuter, Some(T12_JOIN_CONDITION), JoinHint.NONE)))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  /**
   * The use-case when user attempt to chain multiple APPENCOL command in a PPL, this is a common
   * use case, when user prefer to show the statistic report alongside with the dataset.
   */
  test("test multiple AppendCol clauses") {
    val frame = sql(s"""
                       | source = $testTable | FIELDS name, age | APPENDCOL [ stats count() by age | eval m = 1 | FIELDS -m ] | APPENDCOL [FIELDS state]
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("name", "age", "count()", "age", "state")))
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row("Jake", 70, 1, 70, "California"),
        Row("Hello", 30, 1, 30, "New York"),
        Row("John", 25, 1, 25, "Ontario"),
        Row("Jane", 20, 1, 20, "Quebec"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    /*
     :- 'SubqueryAlias T1
     :  +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#544, *]
     :     +- 'Project ['name, 'age]
     :        +- 'UnresolvedRelation [flint_ppl_test], [], false
     */
    val mainSearch = SubqueryAlias(
      "T1",
      Project(
        Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
        Project(
          Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age")),
          RELATION_TEST_TABLE)))

    /*
    +- 'SubqueryAlias T2
     +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#432, *]
        +- 'DataFrameDropColumns ['m]
           +- 'Project [*, 1 AS m#430]
              +- 'Aggregate ['age AS age#429], ['COUNT(*) AS count()#428, 'age AS age#429]
                 +- 'UnresolvedRelation [flint_ppl_test], [], false
     */
    val firstAppenCol = SubqueryAlias(
      "T2",
      Project(
        Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
        DataFrameDropColumns(
          Seq(UnresolvedAttribute("m")),
          Project(
            Seq(UnresolvedStar(None), Alias(Literal(1), "m")()),
            Aggregate(AGE_ALIAS :: Nil, Seq(COUNT_STAR, AGE_ALIAS), RELATION_TEST_TABLE)))))

    val joinWithFirstAppendCol = SubqueryAlias(
      "T1",
      Project(
        Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
        DataFrameDropColumns(
          T12_COLUMNS_SEQ,
          Join(mainSearch, firstAppenCol, LeftOuter, Some(T12_JOIN_CONDITION), JoinHint.NONE))))

    /*
    +- 'SubqueryAlias T2
     +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#553, *]
        +- 'Project ['dept]
           +- 'UnresolvedRelation [flint_ppl_test], [], false
     */
    val secondAppendCol = SubqueryAlias(
      "T2",
      Project(
        Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
        Project(Seq(UnresolvedAttribute("state")), RELATION_TEST_TABLE)))

    val expectedPlan = Project(
      Seq(UnresolvedStar(None)),
      DataFrameDropColumns(
        T12_COLUMNS_SEQ,
        Join(
          joinWithFirstAppendCol,
          secondAppendCol,
          LeftOuter,
          Some(T12_JOIN_CONDITION),
          JoinHint.NONE)))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  /**
   * To simulate the use-case when column `age` present on both main and sub search, with option
   * OVERRIDE=true.
   */
  test("test AppendCol with OVERRIDE option") {
    val frame = sql(s"""
                       | source = $testTable | FIELDS name, age, state | APPENDCOL OVERRIDE=true [stats count() as age]
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("name", "state", "age")))
    // Retrieve the results
    val results: Array[Row] = frame.collect()

    /*
      The sub-search result `APPENDCOL OVERRIDE=true [stats count() as age]` will be attached alongside with first row of main-search,
      however given the non-deterministic natural of nature order, we cannot guarantee which specific data row will be returned from the primary search query.
      Hence, only assert sub-search position but skipping the table content comparison.
     */
    assert(results(0).get(2) == 4)

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
            specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#216, *]
        +- 'Aggregate ['COUNT(*) AS age#240]
           +- 'UnresolvedRelation [flint_ppl_test], [], false
     */
    val t2 = SubqueryAlias(
      "T2",
      Project(
        Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
        Aggregate(
          Nil,
          Seq(
            Alias(
              UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
              "age")()),
          RELATION_TEST_TABLE)))

    val overrideFields =
      Seq(UnresolvedAttribute("T1._row_number_"), UnresolvedAttribute("T1.age"))

    val expectedPlan = Project(
      Seq(UnresolvedStar(None)),
      DataFrameDropColumns(
        T12_COLUMNS_SEQ ++ overrideFields,
        Join(t1, t2, LeftOuter, Some(T12_JOIN_CONDITION), JoinHint.NONE)))
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

}
