/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import java.sql.{Date, Timestamp}

import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{GreaterThan, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLBuiltInDateTimeFunctionITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testPartitionedStateCountryTable =
    "spark_catalog.default.flint_ppl_partitioned_state_country"
  private val testRelativeDateTimeTable = "spark_catalog.default.flint_ppl_relative_datetime"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test tables.
    createPartitionedStateCountryTable(testPartitionedStateCountryTable)
    createRelativeDateTimeTable(testRelativeDateTimeTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test adddate(date, numDays)") {
    val frame = sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `'2020-08-26' + 1` = ADDDATE(DATE('2020-08-26'), 1), `'2020-08-26' + (-1)` = ADDDATE(DATE('2020-08-26'), -1)
                       | | fields `'2020-08-26' + 1`, `'2020-08-26' + (-1)` | head 1
                       | """.stripMargin)
    assertSameRows(Seq(Row(Date.valueOf("2020-08-27"), Date.valueOf("2020-08-25"))), frame)
  }

  test("test subdate(date, numDays)") {
    val frame = sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `'2020-08-26' - 1` = SUBDATE(DATE('2020-08-26'), 1), `'2020-08-26' - (-1)` = SUBDATE(DATE('2020-08-26'), -1)
                       | | fields `'2020-08-26' - 1`, `'2020-08-26' - (-1)` | head 1
                       | """.stripMargin)
    assertSameRows(Seq(Row(Date.valueOf("2020-08-25"), Date.valueOf("2020-08-27"))), frame)
  }

  test("test CURRENT_DATE, CURDATE are synonyms") {
    val frame = sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `CURRENT_DATE` = CURRENT_DATE(), `CURDATE` = CURDATE()
                       | | where CURRENT_DATE = CURDATE
                       | | fields CURRENT_DATE, CURDATE | head 1
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)
  }

  test("test LOCALTIME, LOCALTIMESTAMP, NOW are synonyms") {
    val frame = sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `LOCALTIME` = LOCALTIME(), `LOCALTIMESTAMP` = LOCALTIMESTAMP(), `NOW` = NOW()
                       | | where LOCALTIME = LOCALTIMESTAMP and LOCALTIME = NOW
                       | | fields LOCALTIME, LOCALTIMESTAMP, NOW | head 1
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    assert(results.length == 1)
  }

  test("test DATE, TIMESTAMP") {
    val frame = sql(s"""
                        | source = $testPartitionedStateCountryTable
                        | | eval `DATE('2020-08-26')` = DATE('2020-08-26')
                        | | eval `DATE(TIMESTAMP('2020-08-26 13:49:00'))` = DATE(TIMESTAMP('2020-08-26 13:49:00'))
                        | | eval `DATE('2020-08-26 13:49')` = DATE('2020-08-26 13:49')
                        | | fields `DATE('2020-08-26')`, `DATE(TIMESTAMP('2020-08-26 13:49:00'))`, `DATE('2020-08-26 13:49')`
                        | | head 1
                        | """.stripMargin)
    assertSameRows(
      Seq(
        Row(Date.valueOf("2020-08-26"), Date.valueOf("2020-08-26"), Date.valueOf("2020-08-26"))),
      frame)
  }

  test("test DATE_FORMAT") {
    val frame = sql(s"""
                        | source = $testPartitionedStateCountryTable
                        | | eval format1 = DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), 'yyyy-MMM-dd hh:mm:ss a')
                        | | eval format2 = DATE_FORMAT('1998-01-31 13:14:15.012345', 'HH:mm:ss.SSSSSS')
                        | | fields format1, format2
                        | | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row("1998-Jan-31 01:14:15 PM", "13:14:15.012345")), frame)
  }

  test("test DATEDIFF") {
    val frame = sql(s"""
                        | source = $testPartitionedStateCountryTable
                        | | eval diff1 = DATEDIFF(DATE('2020-08-27'), DATE('2020-08-26'))
                        | | eval diff2 = DATEDIFF(DATE('2020-08-26'), DATE('2020-08-27'))
                        | | eval diff3 = DATEDIFF(DATE('2020-08-27'), DATE('2020-08-27'))
                        | | eval diff4 = DATEDIFF(DATE('2020-08-26'), '2020-08-27')
                        | | eval diff5 = DATEDIFF(TIMESTAMP('2000-01-02 00:00:00'), TIMESTAMP('2000-01-01 23:59:59'))
                        | | eval diff6 = DATEDIFF(DATE('2001-02-01'), TIMESTAMP('2004-01-01 00:00:00'))
                        | | fields diff1, diff2, diff3, diff4, diff5, diff6
                        | | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row(1, -1, 0, -1, 1, -1064)), frame)
  }

  test("test DAY, DAYOFMONTH, DAY_OF_MONTH are synonyms") {
    val frame = sql(s"""
                        | source = $testPartitionedStateCountryTable
                        | | eval `DAY(DATE('2020-08-26'))` = DAY(DATE('2020-08-26'))
                        | | eval `DAYOFMONTH(DATE('2020-08-26'))` = DAYOFMONTH(DATE('2020-08-26'))
                        | | eval `DAY_OF_MONTH(DATE('2020-08-26'))` = DAY_OF_MONTH(DATE('2020-08-26'))
                        | | fields `DAY(DATE('2020-08-26'))`, `DAYOFMONTH(DATE('2020-08-26'))`, `DAY_OF_MONTH(DATE('2020-08-26'))`
                        | | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row(26, 26, 26)), frame)
  }

  test("test DAYOFWEEK, DAY_OF_WEEK are synonyms") {
    val frame = sql(s"""
                        | source = $testPartitionedStateCountryTable
                        | | eval `DAYOFWEEK(DATE('2020-08-26'))` = DAYOFWEEK(DATE('2020-08-26'))
                        | | eval `DAY_OF_WEEK(DATE('2020-08-26'))` = DAY_OF_WEEK(DATE('2020-08-26'))
                        | | fields `DAYOFWEEK(DATE('2020-08-26'))`, `DAY_OF_WEEK(DATE('2020-08-26'))`
                        | | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row(4, 4)), frame)
  }

  test("test DAYOFYEAR, DAY_OF_YEAR are synonyms") {
    val frame = sql(s"""
                        | source = $testPartitionedStateCountryTable
                        | | eval `DAY_OF_YEAR(DATE('2020-08-26'))` = DAY_OF_YEAR(DATE('2020-08-26'))
                        | | eval `DAYOFYEAR(DATE('2020-08-26'))` = DAYOFYEAR(DATE('2020-08-26'))
                        | | fields `DAY_OF_YEAR(DATE('2020-08-26'))`, `DAYOFYEAR(DATE('2020-08-26'))`
                        | | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row(239, 239)), frame)
  }

  test("test WEEK, WEEK_OF_YEAR are synonyms") {
    val frame = sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `WEEK(DATE('2008-02-20'))` = WEEK(DATE('2008-02-20'))
                       | | eval `WEEK_OF_YEAR(DATE('2008-02-20'))` = WEEK_OF_YEAR(DATE('2008-02-20'))
                       | | fields `WEEK(DATE('2008-02-20'))`, `WEEK_OF_YEAR(DATE('2008-02-20'))`
                       | | head 1
                       | """.stripMargin)
    assertSameRows(Seq(Row(8, 8)), frame)
  }

  test("test MONTH, MONTH_OF_YEAR are synonyms") {
    val frame = sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `MONTH(DATE('2020-08-26'))` =  MONTH(DATE('2020-08-26'))
                       | | eval `MONTH_OF_YEAR(DATE('2020-08-26'))` =  MONTH_OF_YEAR(DATE('2020-08-26'))
                       | | fields `MONTH(DATE('2020-08-26'))`, `MONTH_OF_YEAR(DATE('2020-08-26'))`
                       | | head 1
                       | """.stripMargin)
    assertSameRows(Seq(Row(8, 8)), frame)
  }
  test("test WEEKDAY") {
    val frame = sql(s"""
                        | source = $testPartitionedStateCountryTable
                        | | eval `weekday(DATE('2020-08-26'))` = weekday(DATE('2020-08-26'))
                        | | eval `weekday(DATE('2020-08-27'))` = weekday(DATE('2020-08-27'))
                        | | fields `weekday(DATE('2020-08-26'))`, `weekday(DATE('2020-08-27'))`
                        | | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row(2, 3)), frame)
  }

  test("test YEAR") {
    val frame = sql(s"""
                        | source = $testPartitionedStateCountryTable
                        | | eval `YEAR(DATE('2020-08-26'))` = YEAR(DATE('2020-08-26')) | fields `YEAR(DATE('2020-08-26'))`
                        | | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row(2020)), frame)
  }

  test("test from_unixtime and unix_timestamp") {
    val frame = sql(s"""
                       | source = $testPartitionedStateCountryTable |where unix_timestamp(from_unixtime(1700000001)) > 1700000000 | fields name, age
                       | """.stripMargin)
    assertSameRows(
      Seq(Row("Jake", 70), Row("Hello", 30), Row("John", 25), Row("Jane", 20)),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table =
      UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_partitioned_state_country"))
    val filterExpr = GreaterThan(
      UnresolvedFunction(
        "unix_timestamp",
        seq(UnresolvedFunction("from_unixtime", seq(Literal(1700000001)), isDistinct = false)),
        isDistinct = false),
      Literal(1700000000))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test DATE_ADD") {
    val frame1 = sql(s"""
                       | source = $testPartitionedStateCountryTable | eval `'2020-08-26' + 2d` = DATE_ADD(DATE('2020-08-26'), INTERVAL 2 DAY)
                       | | fields `'2020-08-26' + 2d` | head 1
                       | """.stripMargin)
    assertSameRows(Seq(Row(Date.valueOf("2020-08-28"))), frame1)

    val frame2 = sql(s"""
                        | source = $testPartitionedStateCountryTable | eval `'2020-08-26' - 2d` = DATE_ADD(DATE('2020-08-26'), INTERVAL -2 DAY)
                        | | fields `'2020-08-26' - 2d` | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row(Date.valueOf("2020-08-24"))), frame2)

    val frame3 = sql(s"""
                        | source = $testPartitionedStateCountryTable | eval `'2020-08-26' + 2m` = DATE_ADD(DATE('2020-08-26'), INTERVAL 2 MONTH)
                        | | fields `'2020-08-26' + 2m` | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row(Date.valueOf("2020-10-26"))), frame3)

    val frame4 = sql(s"""
                        | source = $testPartitionedStateCountryTable | eval `'2020-08-26' + 2y` = DATE_ADD(DATE('2020-08-26'), INTERVAL 2 YEAR)
                        | | fields `'2020-08-26' + 2y` | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row(Date.valueOf("2022-08-26"))), frame4)

    val ex = intercept[AnalysisException](sql(s"""
                                                 | source = $testPartitionedStateCountryTable | eval `'2020-08-26 01:01:01' + 2h` = DATE_ADD(TIMESTAMP('2020-08-26 01:01:01'), INTERVAL 2 HOUR)
                                                 | | fields `'2020-08-26 01:01:01' + 2h` | head 1
                                                 | """.stripMargin))
    assert(ex.getMessage.contains("""Parameter 1 requires the "DATE" type"""))
  }

  test("test DATE_SUB") {
    val frame1 = sql(s"""
                        | source = $testPartitionedStateCountryTable | eval `'2020-08-26' - 2d` = DATE_SUB(DATE('2020-08-26'), INTERVAL 2 DAY)
                        | | fields `'2020-08-26' - 2d` | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row(Date.valueOf("2020-08-24"))), frame1)

    val frame2 = sql(s"""
                        | source = $testPartitionedStateCountryTable | eval `'2020-08-26' + 2d` = DATE_SUB(DATE('2020-08-26'), INTERVAL -2 DAY)
                        | | fields `'2020-08-26' + 2d` | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row(Date.valueOf("2020-08-28"))), frame2)

    val frame3 = sql(s"""
                        | source = $testPartitionedStateCountryTable | eval `'2020-08-26' - 2m` = DATE_SUB(DATE('2020-08-26'), INTERVAL 12 MONTH)
                        | | fields `'2020-08-26' - 2m` | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row(Date.valueOf("2019-08-26"))), frame3)

    val frame4 = sql(s"""
                        | source = $testPartitionedStateCountryTable | eval `'2020-08-26' - 2y` = DATE_SUB(DATE('2020-08-26'), INTERVAL 2 YEAR)
                        | | fields `'2020-08-26' - 2y` | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row(Date.valueOf("2018-08-26"))), frame4)

    val ex = intercept[AnalysisException](sql(s"""
                                                 | source = $testPartitionedStateCountryTable | eval `'2020-08-26 01:01:01' - 2h` = DATE_SUB(TIMESTAMP('2020-08-26 01:01:01'), INTERVAL 2 HOUR)
                                                 | | fields `'2020-08-26 01:01:01' - 2h` | head 1
                                                 | """.stripMargin))
    assert(ex.getMessage.contains("""Parameter 1 requires the "DATE" type"""))
  }

  test("test TIMESTAMPADD") {
    val frame = sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00')` = TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00')
                       | | eval `TIMESTAMPADD(DAY, 17, TIMESTAMP('2000-01-01 00:00:00'))` = TIMESTAMPADD(DAY, 17, TIMESTAMP('2000-01-01 00:00:00'))
                       | | eval `TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00')` = TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00')
                       | | fields `TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00')`, `TIMESTAMPADD(DAY, 17, TIMESTAMP('2000-01-01 00:00:00'))`, `TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00')`
                       | | head 1
                       | """.stripMargin)
    assertSameRows(
      Seq(
        Row(
          Timestamp.valueOf("2000-01-18 00:00:00"),
          Timestamp.valueOf("2000-01-18 00:00:00"),
          Timestamp.valueOf("1999-10-01 00:00:00"))),
      frame)
  }

  test("test TIMESTAMPDIFF") {
    val frame = sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00')` = TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00')
                       | | eval `TIMESTAMPDIFF(SECOND, TIMESTAMP('2000-01-01 00:00:23'), TIMESTAMP('2000-01-01 00:00:00'))` = TIMESTAMPDIFF(SECOND, TIMESTAMP('2000-01-01 00:00:23'), TIMESTAMP('2000-01-01 00:00:00'))
                       | | fields `TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00')`, `TIMESTAMPDIFF(SECOND, TIMESTAMP('2000-01-01 00:00:23'), TIMESTAMP('2000-01-01 00:00:00'))`
                       | | head 1
                       | """.stripMargin)
    assertSameRows(Seq(Row(4, -23)), frame)
  }

  test("test CURRENT_TIMEZONE") {
    val frame = sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `CURRENT_TIMEZONE` = CURRENT_TIMEZONE()
                       | | fields `CURRENT_TIMEZONE`
                       | """.stripMargin)
    assert(frame.collect().length > 0)
  }

  test("test UTC_TIMESTAMP") {
    val frame = sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `UTC_TIMESTAMP` = UTC_TIMESTAMP()
                       | | fields `UTC_TIMESTAMP`
                       | """.stripMargin)
    assert(frame.collect().length > 0)
  }

  test("test hour, minute, second, HOUR_OF_DAY, MINUTE_OF_HOUR") {
    val frame = sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval h = hour(timestamp('01:02:03')), m = minute(timestamp('01:02:03')), s = second(timestamp('01:02:03'))
                       | | eval hs = hour('2024-07-30 01:02:03'), ms = minute('2024-07-30 01:02:03'), ss = second('01:02:03')
                       | | eval h_d = HOUR_OF_DAY(timestamp('01:02:03')), m_h = MINUTE_OF_HOUR(timestamp('01:02:03')), s_m = SECOND_OF_MINUTE(timestamp('01:02:03'))
                       | | fields h, m, s, hs, ms, ss, h_d, m_h, s_m | head 1
                       | """.stripMargin)
    assertSameRows(Seq(Row(1, 2, 3, 1, 2, 3, 1, 2, 3)), frame)
  }

  test("test LAST_DAY") {
    val frame = sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `last_day('2023-02-06')` = last_day('2023-02-06')
                       | | fields `last_day('2023-02-06')`
                       | | head 1
                       | """.stripMargin)
    assertSameRows(Seq(Row(Date.valueOf("2023-02-28"))), frame)
  }

  test("test MAKE_DATE") {
    val frame = sql(s"""
                        | source = $testPartitionedStateCountryTable
                        | | eval `MAKE_DATE(1945, 5, 9)` = MAKE_DATE(1945, 5, 9) | fields `MAKE_DATE(1945, 5, 9)`
                        | | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row(Date.valueOf("1945-05-09"))), frame)
  }

  test("test QUARTER") {
    val frame = sql(s"""
                        | source = $testPartitionedStateCountryTable
                        | | eval `QUARTER(DATE('2020-08-26'))` = QUARTER(DATE('2020-08-26')) | fields `QUARTER(DATE('2020-08-26'))`
                        | | head 1
                        | """.stripMargin)
    assertSameRows(Seq(Row(3)), frame)
  }

  test("test RELATIVE_TIMESTAMP") {
    val frame = sql(s"""
                       | source=$testRelativeDateTimeTable
                       | | eval relative = relative_timestamp(relative_string)
                       | | sort relative
                       | | fields description, relative_string
                       | """.stripMargin)
    assertSameRows(
      Seq(
        Row("Two weeks ago", "-2wk"),
        Row("Yesterday", "-1d@d"),
        Row("Now", "NOW"),
        Row("Tomorrow", "+D@D"),
        Row("In one month", "+month")),
      frame)
  }

  test("test EARLIEST") {
    var frame = sql(s"""
                       | source=$testRelativeDateTimeTable
                       | | eval relative = relative_timestamp(relative_string)
                       | | sort relative
                       | | where earliest("now",relative)
                       | | fields description, relative_string
                       | """.stripMargin)
    assertSameRows(
      Seq(Row("Now", "NOW"), Row("Tomorrow", "+D@D"), Row("In one month", "+month")),
      frame)

    frame = sql(s"""
                       | source=$testRelativeDateTimeTable
                       | | eval relative = relative_timestamp(relative_string)
                       | | sort relative
                       | | where earliest("+2days",relative)
                       | | fields description, relative_string
                       | """.stripMargin)
    assertSameRows(Seq(Row("In one month", "+month")), frame)
  }

  test("test LATEST") {
    var frame = sql(s"""
                       | source=$testRelativeDateTimeTable
                       | | eval relative = relative_timestamp(relative_string)
                       | | sort relative
                       | | where latest("now",relative)
                       | | fields description, relative_string
                       | """.stripMargin)
    assertSameRows(
      Seq(Row("Two weeks ago", "-2wk"), Row("Yesterday", "-1d@d"), Row("Now", "NOW")),
      frame)

    frame = sql(s"""
                   | source=$testRelativeDateTimeTable
                   | | eval relative = relative_timestamp(relative_string)
                   | | sort relative
                   | | where latest("-2days",relative)
                   | | fields description, relative_string
                   | """.stripMargin)
    assertSameRows(Seq(Row("Two weeks ago", "-2wk")), frame)
  }

  test("test CURRENT_TIME is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `CURRENT_TIME` = CURRENT_TIME()
                       | | fields CURRENT_TIME | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("CURRENT_TIME is not a builtin function of PPL"))
  }

  test("test CONVERT_TZ is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `CONVERT_TZ` = CONVERT_TZ()
                       | | fields CONVERT_TZ | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("CONVERT_TZ is not a builtin function of PPL"))
  }

  test("test ADDTIME is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `ADDTIME` = ADDTIME()
                       | | fields ADDTIME | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("ADDTIME is not a builtin function of PPL"))
  }

  test("test DATETIME is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `DATETIME` = DATETIME()
                       | | fields DATETIME | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("DATETIME is not a builtin function of PPL"))
  }

  test("test DAYNAME is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `DAYNAME` = DAYNAME()
                       | | fields DAYNAME | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("DAYNAME is not a builtin function of PPL"))
  }

  test("test FROM_DAYS is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `FROM_DAYS` = FROM_DAYS()
                       | | fields FROM_DAYS | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("FROM_DAYS is not a builtin function of PPL"))
  }

  test("test GET_FORMAT is not supported") {
    intercept[Exception](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `GET_FORMAT` = GET_FORMAT(DATE, 'USA')
                       | | fields GET_FORMAT | head 1
                       | """.stripMargin))
  }

  test("test MAKETIME is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `MAKETIME` = MAKETIME()
                       | | fields MAKETIME | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("MAKETIME is not a builtin function of PPL"))
  }

  test("test MICROSECOND is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `MICROSECOND` = MICROSECOND()
                       | | fields MICROSECOND | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("MICROSECOND is not a builtin function of PPL"))
  }

  test("test MINUTE_OF_DAY is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `MINUTE_OF_DAY` = MINUTE_OF_DAY()
                       | | fields MINUTE_OF_DAY | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("MINUTE_OF_DAY is not a builtin function of PPL"))
  }

  test("test PERIOD_ADD is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `PERIOD_ADD` = PERIOD_ADD()
                       | | fields PERIOD_ADD | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("PERIOD_ADD is not a builtin function of PPL"))
  }

  test("test PERIOD_DIFF is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `PERIOD_DIFF` = PERIOD_DIFF()
                       | | fields PERIOD_DIFF | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("PERIOD_DIFF is not a builtin function of PPL"))
  }

  test("test SEC_TO_TIME is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `SEC_TO_TIME` = SEC_TO_TIME()
                       | | fields SEC_TO_TIME | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("SEC_TO_TIME is not a builtin function of PPL"))
  }

  test("test STR_TO_DATE is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `STR_TO_DATE` = STR_TO_DATE()
                       | | fields STR_TO_DATE | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("STR_TO_DATE is not a builtin function of PPL"))
  }

  test("test SUBTIME is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `SUBTIME` = SUBTIME()
                       | | fields SUBTIME | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("SUBTIME is not a builtin function of PPL"))
  }

  test("test TIME is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `TIME` = TIME()
                       | | fields TIME | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("TIME is not a builtin function of PPL"))
  }

  test("test TIME_FORMAT is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `TIME_FORMAT` = TIME_FORMAT()
                       | | fields TIME_FORMAT | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("TIME_FORMAT is not a builtin function of PPL"))
  }

  test("test TIME_TO_SEC is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `TIME_TO_SEC` = TIME_TO_SEC()
                       | | fields TIME_TO_SEC | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("TIME_TO_SEC is not a builtin function of PPL"))
  }

  test("test TIMEDIFF is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `TIMEDIFF` = TIMEDIFF()
                       | | fields TIMEDIFF | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("TIMEDIFF is not a builtin function of PPL"))
  }

  test("test TO_DAYS is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `TO_DAYS` = TO_DAYS()
                       | | fields TO_DAYS | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("TO_DAYS is not a builtin function of PPL"))
  }

  test("test TO_SECONDS is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `TO_SECONDS` = TO_SECONDS()
                       | | fields TO_SECONDS | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("TO_SECONDS is not a builtin function of PPL"))
  }

  test("test UTC_DATE is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `UTC_DATE` = UTC_DATE()
                       | | fields UTC_DATE | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("UTC_DATE is not a builtin function of PPL"))
  }

  test("test UTC_TIME is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `UTC_TIME` = UTC_TIME()
                       | | fields UTC_TIME | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("UTC_TIME is not a builtin function of PPL"))
  }

  test("test YEARWEEK is not supported") {
    val ex = intercept[UnsupportedOperationException](sql(s"""
                       | source = $testPartitionedStateCountryTable
                       | | eval `YEARWEEK` = YEARWEEK()
                       | | fields YEARWEEK | head 1
                       | """.stripMargin))
    assert(ex.getMessage.contains("YEARWEEK is not a builtin function of PPL"))
  }
}
