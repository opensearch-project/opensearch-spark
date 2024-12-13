/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.opensearch.sql.ppl.utils.SortUtils

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{
  UnresolvedAttribute,
  UnresolvedFunction,
  UnresolvedRelation,
  UnresolvedStar
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  And,
  CurrentRow,
  EqualTo,
  Literal,
  RowFrame,
  RowNumber,
  SpecifiedWindowFrame,
  UnboundedPreceding,
  WindowExpression,
  WindowSpecDefinition
}
import org.apache.spark.sql.catalyst.plans.{LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._

import org.scalatest.matchers.should.Matchers

class PPLLogicalPlanAppendColCommandTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  private val ROW_NUMBER_AGGREGATION = Alias(
    WindowExpression(
      RowNumber(),
      WindowSpecDefinition(
        Nil,
        SortUtils.sortOrder(Literal("1"), false) :: Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))),
    "_row_number_")()

  private val COUNT_STAR = Alias(
    UnresolvedFunction(Seq("COUNT"),
    Seq(UnresolvedStar(None)),
    isDistinct = false),
    "count()")()

  private val AGE_ALIAS = Alias(UnresolvedAttribute("age"), "age")()

  private val RELATION_EMPLOYEES = UnresolvedRelation(Seq("employees"))

  private val T12_JOIN_CONDITION = EqualTo(
    UnresolvedAttribute("T1._row_number_"), UnresolvedAttribute("T2._row_number_"))

  private val T12_COLUMNS_SEQ = Seq(
    UnresolvedAttribute("T1._row_number_"), UnresolvedAttribute("T2._row_number_"))

  /**
   * Expected:
   'Project [*]
   +- 'DataFrameDropColumns ['T1._row_number_, 'T2._row_number_]
   +- 'Join LeftOuter, ('T1._row_number_ = 'T2._row_number_)
   :- 'SubqueryAlias T1
   :  +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#1, *]
   :     +- 'UnresolvedRelation [employees], [], false
   +- 'SubqueryAlias T2
   +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#5, *]
   +- 'Aggregate ['age AS age#3], ['COUNT(*) AS count()#2, 'age AS age#3]
   +- 'UnresolvedRelation [employees], [], false
   */
  test("test AppendCol with NO transformation on main") {
    val context = new CatalystPlanContext
    val logicalPlan = planTransformer.visit(
      plan(pplParser, "source=employees | APPENDCOL [stats count() by age];"),
      context
    )

    /*
      :- 'SubqueryAlias T1
      :  +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#7, *]
      :     +- 'UnresolvedRelation [relation], [], false
     */
    val t1 = SubqueryAlias("T1", Project(
        Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
        RELATION_EMPLOYEES))

    /*
    +- 'SubqueryAlias T2
      +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST,
          specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#11, *]
        +- 'Aggregate ['age AS age#9], ['COUNT(*) AS count()#8, 'age AS age#10]
           +- 'UnresolvedRelation [relation], [], false
     */
    val t2 = SubqueryAlias("T2", Project(
      Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
      Aggregate(
        AGE_ALIAS :: Nil, Seq(COUNT_STAR, AGE_ALIAS),
        RELATION_EMPLOYEES)
    ))

    val result = Project(Seq(UnresolvedStar(None)),
        DataFrameDropColumns(T12_COLUMNS_SEQ,
          Join(t1, t2, LeftOuter, Some(T12_JOIN_CONDITION), JoinHint.NONE)))

    comparePlans(logicalPlan, result, checkAnalysis = false)
  }

  /**
   * 'Project [*]
   * +- 'DataFrameDropColumns ['T1._row_number_, 'T2._row_number_]
   * +- 'Join LeftOuter, ('T1._row_number_ = 'T2._row_number_)
   * :- 'SubqueryAlias T1
   * :  +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#11, *]
   * :     +- 'Project ['age, 'dept, 'salary]
   * :        +- 'UnresolvedRelation [relation], [], false
   * +- 'SubqueryAlias T2
   * +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#15, *]
   * +- 'Aggregate ['age AS age#13], ['COUNT(*) AS count()#12, 'age AS age#13]
   * +- 'UnresolvedRelation [relation], [], false
   */
  test("test AppendCol with transformation on main-search") {
    val context = new CatalystPlanContext
    val logicalPlan = planTransformer.visit(
      plan(pplParser, "source=employees | FIELDS age, dept, salary | APPENDCOL [stats count() by age];"),
      context
    )

    /*
      :- 'SubqueryAlias T1
      :  +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST,
                specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#11, *]
      :     +- 'Project ['age, 'dept, 'salary]
      :        +- 'UnresolvedRelation [relation], [], false
     */
    val t1 = SubqueryAlias("T1", Project(
      Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
      Project(Seq(
        UnresolvedAttribute("age"),
        UnresolvedAttribute("dept"),
        UnresolvedAttribute("salary")), RELATION_EMPLOYEES)))


    /*
    +- 'SubqueryAlias T2
      +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST,
          specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#11, *]
        +- 'Aggregate ['age AS age#9], ['COUNT(*) AS count()#8, 'age AS age#10]
           +- 'UnresolvedRelation [relation], [], false
     */
    val t2 = SubqueryAlias("T2", Project(
      Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
      Aggregate(
        AGE_ALIAS :: Nil, Seq(COUNT_STAR, AGE_ALIAS),
        RELATION_EMPLOYEES)
    ))

    val result = Project(Seq(UnresolvedStar(None)),
      DataFrameDropColumns(T12_COLUMNS_SEQ,
        Join(t1, t2, LeftOuter, Some(T12_JOIN_CONDITION), JoinHint.NONE)))

    comparePlans(logicalPlan, result, checkAnalysis = false)
  }

  /**
   * 'Project [*]
   * +- 'DataFrameDropColumns ['T1._row_number_, 'T2._row_number_]
   * +- 'Join LeftOuter, ('T1._row_number_ = 'T2._row_number_)
   * :- 'SubqueryAlias T1
   * :  +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#427, *]
   * :     +- 'Project ['age, 'dept, 'salary]
   * :        +- 'UnresolvedRelation [employees], [], false
   * +- 'SubqueryAlias T2
   *  +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#432, *]
   *    +- 'DataFrameDropColumns ['m]
   *      +- 'Project [*, 1 AS m#430]
   *        +- 'Aggregate ['age AS age#429], ['COUNT(*) AS count()#428, 'age AS age#429]
   *          +- 'UnresolvedRelation [employees], [], false
   */
  test("test AppendCol with chained sub-search") {
    val context = new CatalystPlanContext
    val logicalPlan = planTransformer.visit(
      plan(pplParser, "source=employees | FIELDS age, dept, salary | APPENDCOL [ stats count() by age | eval m = 1 | FIELDS -m ];"),
      context
    )

    /*
      :- 'SubqueryAlias T1
      :  +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST,
                specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#11, *]
      :     +- 'Project ['age, 'dept, 'salary]
      :        +- 'UnresolvedRelation [relation], [], false
     */
    val t1 = SubqueryAlias("T1", Project(
      Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
      Project(Seq(
        UnresolvedAttribute("age"),
        UnresolvedAttribute("dept"),
        UnresolvedAttribute("salary")), RELATION_EMPLOYEES)))


    /*
    +- 'SubqueryAlias T2
     +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#432, *]
        +- 'DataFrameDropColumns ['m]
           +- 'Project [*, 1 AS m#430]
              +- 'Aggregate ['age AS age#429], ['COUNT(*) AS count()#428, 'age AS age#429]
                 +- 'UnresolvedRelation [employees], [], false
     */
    val t2 = SubqueryAlias("T2", Project(
      Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
      DataFrameDropColumns(Seq(UnresolvedAttribute("m")),
        Project(Seq(UnresolvedStar(None), Alias(Literal(1),"m")()),
          Aggregate(
            AGE_ALIAS :: Nil, Seq(COUNT_STAR, AGE_ALIAS),
            RELATION_EMPLOYEES)))
    ))

    val result = Project(Seq(UnresolvedStar(None)),
      DataFrameDropColumns(T12_COLUMNS_SEQ,
        Join(t1, t2, LeftOuter, Some(T12_JOIN_CONDITION), JoinHint.NONE)))

    comparePlans(logicalPlan, result, checkAnalysis = false)
  }

  /**
   * == Parsed Logical Plan ==
   * 'Project [*]
   * +- 'DataFrameDropColumns ['T1._row_number_, 'T2._row_number_]
   * +- 'Join LeftOuter, ('T1._row_number_ = 'T2._row_number_)
   * :- 'SubqueryAlias T1
   * :  +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#551, *]
   * :     +- 'DataFrameDropColumns ['T1._row_number_, 'T2._row_number_]
   * :        +- 'Join LeftOuter, ('T1._row_number_ = 'T2._row_number_)
   * :           :- 'SubqueryAlias T1
   * :           :  +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#544, *]
   * :           :     +- 'Project ['name, 'age]
   * :           :        +- 'UnresolvedRelation [employees], [], false
   * :           +- 'SubqueryAlias T2
   * :              +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#549, *]
   * :                 +- 'DataFrameDropColumns ['m]
   * :                    +- 'Project [*, 1 AS m#547]
   * :                       +- 'Aggregate ['age AS age#546], ['COUNT(*) AS count()#545, 'age AS age#546]
   * :                          +- 'UnresolvedRelation [employees], [], false
   * +- 'SubqueryAlias T2
   *  +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#553, *]
   *    +- 'Project ['dept]
   *      +- 'UnresolvedRelation [employees], [], false
   */
  test("test multiple AppendCol clauses") {
    val context = new CatalystPlanContext
    val logicalPlan = planTransformer.visit(
      plan(pplParser, "source=employees | FIELDS name, age | APPENDCOL [ stats count() by age | eval m = 1 | FIELDS -m ] | APPENDCOL [FIELDS dept];"),
      context
    )

    /*
      :- 'SubqueryAlias T1
      :  +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#544, *]
      :     +- 'Project ['name, 'age]
      :        +- 'UnresolvedRelation [employees], [], false
     */
    val mainSearch = SubqueryAlias("T1", Project(
      Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
      Project(Seq(
        UnresolvedAttribute("name"),
        UnresolvedAttribute("age")), RELATION_EMPLOYEES)))


    /*
    +- 'SubqueryAlias T2
     +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#432, *]
        +- 'DataFrameDropColumns ['m]
           +- 'Project [*, 1 AS m#430]
              +- 'Aggregate ['age AS age#429], ['COUNT(*) AS count()#428, 'age AS age#429]
                 +- 'UnresolvedRelation [employees], [], false
     */
    val firstAppenCol = SubqueryAlias("T2", Project(
      Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
      DataFrameDropColumns(Seq(UnresolvedAttribute("m")),
        Project(Seq(UnresolvedStar(None), Alias(Literal(1),"m")()),
          Aggregate(
            AGE_ALIAS :: Nil, Seq(COUNT_STAR, AGE_ALIAS),
            RELATION_EMPLOYEES)))
    ))


    val joinWithFirstAppendCol = SubqueryAlias("T1", Project(Seq(
        ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
      DataFrameDropColumns(T12_COLUMNS_SEQ,
        Join(mainSearch, firstAppenCol, LeftOuter, Some(T12_JOIN_CONDITION), JoinHint.NONE))))


    /*
    +- 'SubqueryAlias T2
     +- 'Project [row_number() windowspecdefinition(1 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_#553, *]
        +- 'Project ['dept]
           +- 'UnresolvedRelation [employees], [], false
     */
    val secondAppendCol = SubqueryAlias("T2", Project(
      Seq(ROW_NUMBER_AGGREGATION, UnresolvedStar(None)),
      Project(Seq(
        UnresolvedAttribute("dept")), RELATION_EMPLOYEES)))


    val joinWithSecondAppendCol = Project(Seq(UnresolvedStar(None)),
      DataFrameDropColumns(T12_COLUMNS_SEQ,
        Join(joinWithFirstAppendCol, secondAppendCol, LeftOuter, Some(T12_JOIN_CONDITION), JoinHint.NONE)))

    comparePlans(logicalPlan, joinWithSecondAppendCol, checkAnalysis = false)
  }

}
