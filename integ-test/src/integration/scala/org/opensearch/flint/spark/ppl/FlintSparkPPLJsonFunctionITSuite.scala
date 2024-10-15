/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo, Literal, Not}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLJsonFunctionITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"

  private val validJson1 = "{\"account_number\":1,\"balance\":39225,\"age\":32,\"gender\":\"M\"}"
  private val validJson2 = "{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\",\"f4\":\"b\"}}"
  private val validJson3 = "[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]"
  private val validJson4 = "[]"
  private val validJson5 =
    "{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}"
  private val invalidJson1 = "[1,2"
  private val invalidJson2 = "[invalid json]"
  private val invalidJson3 = "{\"invalid\": \"json\""
  private val invalidJson4 = "invalid json"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create test table
    createNullableJsonContentTable(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test json() function: valid JSON") {
    Seq(validJson1, validJson2, validJson3, validJson4, validJson5).foreach { jsonStr =>
      val frame = sql(s"""
                         | source = $testTable
                         | | eval result = json('$jsonStr') | head 1 | fields result
                         | """.stripMargin)
      val results: Array[Row] = frame.collect()
      val expectedResults: Array[Row] = Array(Row(jsonStr))
      assert(results.sameElements(expectedResults))

      val logicalPlan: LogicalPlan = frame.queryExecution.logical
      val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
      val jsonFunc = Alias(
        UnresolvedFunction(
          "get_json_object",
          Seq(Literal(jsonStr), Literal("$")),
          isDistinct = false),
        "result")()
      val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
      val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
      val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
      comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
    }
  }

  test("test json() function: invalid JSON") {
    Seq(invalidJson1, invalidJson2, invalidJson3, invalidJson4).foreach { jsonStr =>
      val frame = sql(s"""
                         | source = $testTable
                         | | eval result = json('$jsonStr') | head 1 | fields result
                         | """.stripMargin)
      val results: Array[Row] = frame.collect()
      val expectedResults: Array[Row] = Array(Row(null))
      assert(results.sameElements(expectedResults))

      val logicalPlan: LogicalPlan = frame.queryExecution.logical
      val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
      val jsonFunc = Alias(
        UnresolvedFunction(
          "get_json_object",
          Seq(Literal(jsonStr), Literal("$")),
          isDistinct = false),
        "result")()
      val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
      val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
      val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
      comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
    }
  }

  test("test json() function on field") {
    val frame = sql(s"""
                       | source = $testTable
                       | | where isValid = true | eval result = json(jString) | fields result
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Seq(validJson1, validJson2, validJson3, validJson4, validJson5).map(Row.apply(_)).toArray
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val frame2 = sql(s"""
                       | source = $testTable
                       | | where isValid = false | eval result = json(jString) | fields result
                       | """.stripMargin)
    val results2: Array[Row] = frame2.collect()
    val expectedResults2: Array[Row] =
      Array(Row(null), Row(null), Row(null), Row(null), Row(null))
    assert(results2.sameElements(expectedResults2))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val jsonFunc = Alias(
      UnresolvedFunction(
        "get_json_object",
        Seq(UnresolvedAttribute("jString"), Literal("$")),
        isDistinct = false),
      "result")()
    val eval = Project(
      Seq(UnresolvedStar(None), jsonFunc),
      Filter(EqualTo(UnresolvedAttribute("isValid"), Literal(true)), table))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), eval)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_array()") {
    // test string array
    var frame = sql(s"""
                   | source = $testTable | eval result = json_array('this', 'is', 'a', 'string', 'array') | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(
      Seq(Row(Array("this", "is", "a", "string", "array"))),
      frame.collect().toSeq)

    // test empty array
    frame = sql(s"""
                   | source = $testTable | eval result = json_array() | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(Seq(Row(Array())), frame.collect().toSeq)

    // test number array
    frame = sql(s"""
                   | source = $testTable | eval result = json_array(1, 2, 0, -1, 1.1, -0.11) | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(Seq(Row(Array(1.0, 2.0, 0.0, -1.0, 1.1, -0.11))), frame.collect().toSeq)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val jsonFunc = Alias(
      UnresolvedFunction(
        "to_json",
        Seq(
          UnresolvedFunction(
            "array",
            Seq(Literal(1), Literal(2), Literal(0), Literal(-1), Literal(1.1), Literal(-0.11)),
            isDistinct = false)),
        isDistinct = false),
      "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)

    // item in json_array should all be the same type
    val ex = intercept[AnalysisException](sql(s"""
                   | source = $testTable | eval result = json_array('this', 'is', 1.1, -0.11, true, false) | head 1 | fields result
                   | """.stripMargin))
    assert(ex.getMessage().contains("should all be the same type"))
  }

  test("test json_array_length()") {
    var frame = sql(s"""
                   | source = $testTable | eval result = json_array_length(json_array('this', 'is', 'a', 'string', 'array')) | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(Seq(Row(5)), frame.collect().toSeq)

    frame = sql(s"""
                   | source = $testTable | eval result = json_array_length(json_array(1, 2, 0, -1, 1.1, -0.11)) | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(Seq(Row(6)), frame.collect().toSeq)

    frame = sql(s"""
                   | source = $testTable | eval result = json_array_length(json_array()) | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(Seq(Row(0)), frame.collect().toSeq)

    frame = sql(s"""
                   | source = $testTable | eval result = json_array_length('[]') | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(Seq(Row(0)), frame.collect().toSeq)
    frame = sql(s"""
                   | source = $testTable | eval result = json_array_length('[1,2,3,4]') | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(Seq(Row(4)), frame.collect().toSeq)
    frame = sql(s"""
                   | source = $testTable | eval result = json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]') | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(Seq(Row(5)), frame.collect().toSeq)
    frame = sql(s"""
                   | source = $testTable | eval result = json_array_length('{\"key\": 1}') | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(Seq(Row(null)), frame.collect().toSeq)
    frame = sql(s"""
                   | source = $testTable | eval result = json_array_length('[1,2') | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(Seq(Row(null)), frame.collect().toSeq)
  }

  test("test json_object()") {
    // test value is a string
    var frame = sql(s"""
                   | source = $testTable | eval result = json_object('key', 'string') | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(Seq(Row("""{"key":"string"}""")), frame.collect().toSeq)

    // test value is a number
    frame = sql(s"""
                   | source = $testTable | eval result = json_object('key', 123.45) | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(Seq(Row("""{"key":123.45}""")), frame.collect().toSeq)

    // test value is a boolean
    frame = sql(s"""
                   | source = $testTable | eval result = json_object('key', true) | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(Seq(Row("""{"key":true}""")), frame.collect().toSeq)

    // test value is an empty array
    frame = sql(s"""
                   | source = $testTable | eval result = json_object('key', json_array()) | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(Seq(Row("""{"key":[]}""")), frame.collect().toSeq)

    // test value is an array
    frame = sql(s"""
                   | source = $testTable | eval result = json_object('key', json_array(1, 2, 3)) | head 1 | fields result
                   | """.stripMargin)
    QueryTest.sameRows(Seq(Row("""{"key":[1,2,3]}""")), frame.collect().toSeq)

    // test value is an another json
    frame = sql(s"""
                   | source = $testTable
                   | | where isValid = true
                   | | eval result = json_object('key', json(jString)) | fields result
                   | """.stripMargin)
    val expectedRows = Seq(
      Row("""{"key":"{"account_number":1,"balance":39225,"age":32,"gender":"M"}"""),
      Row("""{"key":{"f1":"abc", "f2":{"f3":"a", "f4":"b"}}}"""),
      Row("""{"key":[1, 2, 3, {"f1":1, "f2":[5, 6]}, 4]}"""),
      Row("""{"key":[]}"""),
      Row(
        """{"key":{"teacher":"Alice", "student":[{"name":"Bob", "rank":1}, {"name":"Charlie", "rank":2}]}}"""))
    QueryTest.sameRows(expectedRows, frame.collect().toSeq)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val jsonFunc = Alias(
      UnresolvedFunction(
        "to_json",
        Seq(
          UnresolvedFunction(
            "named_struct",
            Seq(
              Literal("key"),
              UnresolvedFunction(
                "get_json_object",
                Seq(UnresolvedAttribute("jString"), Literal("$")),
                isDistinct = false)),
            isDistinct = false)),
        isDistinct = false),
      "result")()
    val eval = Project(
      Seq(UnresolvedStar(None), jsonFunc),
      Filter(EqualTo(UnresolvedAttribute("isValid"), Literal(true)), table))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), eval)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_valid()") {
    val frame = sql(s"""
                       | source = $testTable
                       | | where json_valid(jString) | fields jString
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Seq(validJson1, validJson2, validJson3, validJson4, validJson5).map(Row.apply(_)).toArray
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val frame2 = sql(s"""
                        | source = $testTable
                        | | where not json_valid(jString) | fields jString
                        | """.stripMargin)
    val results2: Array[Row] = frame2.collect()
    val expectedResults2: Array[Row] =
      Seq(invalidJson1, invalidJson2, invalidJson3, invalidJson4, null).map(Row.apply(_)).toArray
    assert(results2.sameElements(expectedResults2))

    val logicalPlan: LogicalPlan = frame2.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val jsonFunc =
      UnresolvedFunction(
        "isnotnull",
        Seq(
          UnresolvedFunction(
            "get_json_object",
            Seq(UnresolvedAttribute("jString"), Literal("$")),
            isDistinct = false)),
        isDistinct = false)
    val where = Filter(Not(jsonFunc), table)
    val expectedPlan = Project(Seq(UnresolvedAttribute("jString")), where)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_keys()") {
    val frame = sql(s"""
                       | source = $testTable
                       | | where isValid = true
                       | | eval result = json_keys(json(jString)) | fields result
                       | """.stripMargin)
    val expectedRows = Seq(
      Row(Array("account_number", "balance", "age", "gender")),
      Row(Array("f1", "f2")),
      Row(null),
      Row(null),
      Row(Array("teacher", "student")))
    QueryTest.sameRows(expectedRows, frame.collect().toSeq)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val jsonFunc = Alias(
      UnresolvedFunction(
        "json_object_keys",
        Seq(
          UnresolvedFunction(
            "get_json_object",
            Seq(UnresolvedAttribute("jString"), Literal("$")),
            isDistinct = false)),
        isDistinct = false),
      "result")()
    val eval = Project(
      Seq(UnresolvedStar(None), jsonFunc),
      Filter(EqualTo(UnresolvedAttribute("isValid"), Literal(true)), table))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), eval)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_extract()") {

  }
}
