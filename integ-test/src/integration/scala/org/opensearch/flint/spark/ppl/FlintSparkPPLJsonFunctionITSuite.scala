/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import java.util

import org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_APPEND
import org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_DELETE
import org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_EXTEND
import org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_SET
import org.opensearch.sql.expression.function.SerializableUdf.visit

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
  private val validJson6 = "[1,2,3]"
  private val validJson7 =
    "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}"
  private val validJson8 =
    "{\"school\":{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}"
  private val validJson9 = "{\"a\":[\"valueA\", \"valueB\"]}"
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
      assertSameRows(Seq(Row(jsonStr)), frame)

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
      assertSameRows(Seq(Row(null)), frame)

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
    assertSameRows(
      Seq(validJson1, validJson2, validJson3, validJson4, validJson5, validJson6).map(
        Row.apply(_)),
      frame)

    val frame2 = sql(s"""
                       | source = $testTable
                       | | where isValid = false | eval result = json(jString) | fields result
                       | """.stripMargin)
    assertSameRows(Seq(Row(null), Row(null), Row(null), Row(null), Row(null)), frame2)

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
    assertSameRows(Seq(Row(Seq("this", "is", "a", "string", "array").toArray)), frame)

    // test empty array
    frame = sql(s"""
                   | source = $testTable | eval result = json_array() | head 1 | fields result
                   | """.stripMargin)
    assertSameRows(Seq(Row(Array.empty)), frame)

    // test number array
    frame = sql(s"""
                   | source = $testTable | eval result = json_array(1, 2, 0, -1, 1.1, -0.11) | head 1 | fields result
                   | """.stripMargin)
    assertSameRows(Seq(Row(Seq(1.0, 2.0, 0.0, -1.0, 1.1, -0.11).toArray)), frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val jsonFunc = Alias(
      UnresolvedFunction(
        "array",
        Seq(Literal(1), Literal(2), Literal(0), Literal(-1), Literal(1.1), Literal(-0.11)),
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

  test("test json_array() with to_json_tring()") {
    val frame = sql(s"""
                       | source = $testTable | eval result = to_json_string(json_array(1,2,0,-1,1.1,-0.11)) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(Seq(Row("""[1.0,2.0,0.0,-1.0,1.1,-0.11]""")), frame)
  }

  test("test array_length()") {
    var frame = sql(s"""
         | source = $testTable| eval result = array_length(json_array('this', 'is', 'a', 'string', 'array')) | head 1 | fields result
         | """.stripMargin)
    assertSameRows(Seq(Row(5)), frame)

    frame = sql(s"""
         | source = $testTable| eval result = array_length(json_array(1, 2, 0, -1, 1.1, -0.11)) | head 1 | fields result
         | """.stripMargin)
    assertSameRows(Seq(Row(6)), frame)

    frame = sql(s"""
         | source = $testTable| eval result = array_length(json_array()) | head 1 | fields result
         | """.stripMargin)
    assertSameRows(Seq(Row(0)), frame)
  }

  test("test json_array_length()") {
    var frame = sql(s"""
                   | source = $testTable | eval result = json_array_length('[]') | head 1 | fields result
                   | """.stripMargin)
    assertSameRows(Seq(Row(0)), frame)
    frame = sql(s"""
                   | source = $testTable | eval result = json_array_length('[1,2,3,4]') | head 1 | fields result
                   | """.stripMargin)
    assertSameRows(Seq(Row(4)), frame)
    frame = sql(s"""
                   | source = $testTable | eval result = json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]') | head 1 | fields result
                   | """.stripMargin)
    assertSameRows(Seq(Row(5)), frame)
    frame = sql(s"""
                   | source = $testTable | eval result = json_array_length('{\"key\": 1}') | head 1 | fields result
                   | """.stripMargin)
    assertSameRows(Seq(Row(null)), frame)
    frame = sql(s"""
                   | source = $testTable | eval result = json_array_length('[1,2') | head 1 | fields result
                   | """.stripMargin)
    assertSameRows(Seq(Row(null)), frame)
  }

  test("test json_object()") {
    // test value is a string
    var frame = sql(s"""
         | source = $testTable| eval result = to_json_string(json_object('key', 'string_value')) | head 1 | fields result
         | """.stripMargin)
    assertSameRows(Seq(Row("""{"key":"string_value"}""")), frame)

    // test value is a number
    frame = sql(s"""
         | source = $testTable| eval result = to_json_string(json_object('key', 123.45)) | head 1 | fields result
         | """.stripMargin)
    assertSameRows(Seq(Row("""{"key":123.45}""")), frame)

    // test value is a boolean
    frame = sql(s"""
         | source = $testTable| eval result = to_json_string(json_object('key', true)) | head 1 | fields result
         | """.stripMargin)
    assertSameRows(Seq(Row("""{"key":true}""")), frame)

    frame = sql(s"""
         | source = $testTable| eval result = to_json_string(json_object("a", 1, "b", 2, "c", 3)) | head 1 | fields result
         | """.stripMargin)
    assertSameRows(Seq(Row("""{"a":1,"b":2,"c":3}""")), frame)
  }

  test("test json_object() and json_array()") {
    // test value is an empty array
    var frame = sql(s"""
         | source = $testTable| eval result = to_json_string(json_object('key', array())) | head 1 | fields result
         | """.stripMargin)
    assertSameRows(Seq(Row("""{"key":[]}""")), frame)

    // test value is an array
    frame = sql(s"""
         | source = $testTable| eval result = to_json_string(json_object('key', array(1, 2, 3))) | head 1 | fields result
         | """.stripMargin)
    assertSameRows(Seq(Row("""{"key":[1,2,3]}""")), frame)

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
                "array",
                Seq(Literal(1), Literal(2), Literal(3)),
                isDistinct = false)),
            isDistinct = false)),
        isDistinct = false),
      "result")()
    var expectedPlan = Project(
      Seq(UnresolvedAttribute("result")),
      GlobalLimit(
        Literal(1),
        LocalLimit(Literal(1), Project(Seq(UnresolvedStar(None), jsonFunc), table))))
    comparePlans(frame.queryExecution.logical, expectedPlan, checkAnalysis = false)
  }

  test("test json_object() nested") {
    val frame = sql(s"""
                   | source = $testTable | eval result = to_json_string(json_object('outer', json_object('inner', 123.45))) | head 1 | fields result
                   | """.stripMargin)
    assertSameRows(Seq(Row("""{"outer":{"inner":123.45}}""")), frame)
  }

  test("test json_object(), json_array() and json()") {
    val frame = sql(s"""
                       | source = $testTable | eval result = to_json_string(json_object("array", json_array(1,2,0,-1,1.1,-0.11))) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(Seq(Row("""{"array":[1.0,2.0,0.0,-1.0,1.1,-0.11]}""")), frame)
  }

  test("test json_valid()") {
    val frame = sql(s"""
                       | source = $testTable
                       | | where json_valid(jString) | fields jString
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Seq(validJson1, validJson2, validJson3, validJson4, validJson5, validJson6)
        .map(Row.apply(_))
        .toArray
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
      Row(Array("teacher", "student")),
      Row(null))
    assertSameRows(expectedRows, frame)

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
    val frame = sql("""
                      | source = spark_catalog.default.flint_ppl_test | where id = 5
                      | | eval root = json_extract(jString, '$')
                      | | eval teacher = json_extract(jString, '$.teacher')
                      | | eval students = json_extract(jString, '$.student')
                      | | eval students_* = json_extract(jString, '$.student[*]')
                      | | eval student_0 = json_extract(jString, '$.student[0]')
                      | | eval student_names = json_extract(jString, '$.student[*].name')
                      | | eval student_1_name = json_extract(jString, '$.student[1].name')
                      | | eval student_non_exist_key = json_extract(jString, '$.student[0].non_exist_key')
                      | | eval student_non_exist = json_extract(jString, '$.student[10]')
                      | | fields root, teacher, students, students_*, student_0, student_names, student_1_name, student_non_exist_key, student_non_exist
                      | """.stripMargin)
    val expectedSeq = Seq(
      Row(
        """{"teacher":"Alice","student":[{"name":"Bob","rank":1},{"name":"Charlie","rank":2}]}""",
        "Alice",
        """[{"name":"Bob","rank":1},{"name":"Charlie","rank":2}]""",
        """[{"name":"Bob","rank":1},{"name":"Charlie","rank":2}]""",
        """{"name":"Bob","rank":1}""",
        """["Bob","Charlie"]""",
        "Charlie",
        null,
        null))
    assertSameRows(expectedSeq, frame)
  }

  test("test json_delete() function: one key") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_delete('$validJson1',array('age')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(Seq(Row("{\"account_number\":1,\"balance\":39225,\"gender\":\"M\"}")), frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression = UnresolvedFunction("array", Seq(Literal("age")), isDistinct = false)
    val jsonObjExp =
      Literal("{\"account_number\":1,\"balance\":39225,\"age\":32,\"gender\":\"M\"}")
    val jsonFunc =
      Alias(visit(JSON_DELETE, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_delete() function: multiple keys") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_delete('$validJson1',array('age','gender')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(Seq(Row("{\"account_number\":1,\"balance\":39225}")), frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction("array", Seq(Literal("age"), Literal("gender")), isDistinct = false)
    val jsonObjExp =
      Literal("{\"account_number\":1,\"balance\":39225,\"age\":32,\"gender\":\"M\"}")
    val jsonFunc =
      Alias(visit(JSON_DELETE, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_delete() function: nested key") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_delete('$validJson2',array('f2.f3')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(Seq(Row("{\"f1\":\"abc\",\"f2\":{\"f4\":\"b\"}}")), frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction("array", Seq(Literal("f2.f3")), isDistinct = false)
    val jsonObjExp =
      Literal("{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\",\"f4\":\"b\"}}")
    val jsonFunc =
      Alias(visit(JSON_DELETE, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_delete() function: multi depth keys ") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_delete('$validJson5',array('teacher', 'student.rank')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(Seq(Row("{\"student\":[{\"name\":\"Bob\"},{\"name\":\"Charlie\"}]}")), frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction(
        "array",
        Seq(Literal("teacher"), Literal("student.rank")),
        isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")
    val jsonFunc =
      Alias(visit(JSON_DELETE, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_delete() function: key not found") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_delete('$validJson5',array('none')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction("array", Seq(Literal("none")), isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")
    val jsonFunc =
      Alias(visit(JSON_DELETE, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  // JSON_SET

  test("test json_set() function: one key") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_set('$validJson1',array('age', '42')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row("{\"account_number\":1,\"balance\":39225,\"age\":42,\"gender\":\"M\"}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction("array", Seq(Literal("age"), Literal("42")), isDistinct = false)
    val jsonObjExp =
      Literal("{\"account_number\":1,\"balance\":39225,\"age\":32,\"gender\":\"M\"}")
    val jsonFunc =
      Alias(visit(JSON_SET, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_set() function: multiple keys") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_set('$validJson1',array('age','42','name','\"Foobar\"')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"account_number\":1,\"balance\":39225,\"age\":42,\"gender\":\"M\",\"name\":\"Foobar\"}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction(
        "array",
        Seq(Literal("age"), Literal("42"), Literal("name"), Literal("\"Foobar\"")),
        isDistinct = false)
    val jsonObjExp =
      Literal("{\"account_number\":1,\"balance\":39225,\"age\":32,\"gender\":\"M\"}")
    val jsonFunc =
      Alias(visit(JSON_SET, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_set() function: nested key") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_set('$validJson2',array('f2.f3','"zzz"')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(Seq(Row("{\"f1\":\"abc\",\"f2\":{\"f3\":\"zzz\",\"f4\":\"b\"}}")), frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction("array", Seq(Literal("f2.f3"), Literal("\"zzz\"")), isDistinct = false)
    val jsonObjExp =
      Literal("{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\",\"f4\":\"b\"}}")
    val jsonFunc =
      Alias(visit(JSON_SET, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_set() function: set new key") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_set('$validJson5',array('grade','8')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}],\"grade\":8}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction("array", Seq(Literal("grade"), Literal("8")), isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")
    val jsonFunc =
      Alias(visit(JSON_SET, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  // JSON_APPEND

  test("test json_append() function: add single value") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_append('$validJson7',array('teacher', '"Tom"')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"teacher\":[\"Alice\",\"Tom\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction("array", Seq(Literal("teacher"), Literal("\"Tom\"")), isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")
    val jsonFunc =
      Alias(visit(JSON_APPEND, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_append() function: add single value key not found") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_append('$validJson7',array('headmaster', '"Tom"')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}],\"headmaster\":[\"Tom\"]}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction(
        "array",
        Seq(Literal("headmaster"), Literal("\"Tom\"")),
        isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")
    val jsonFunc =
      Alias(visit(JSON_APPEND, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_append() function: add single Object key not found") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_append('$validJson7',array('headmaster', '{"name":"Tomy","rank":1}')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}],\"headmaster\":[{\"name\":\"Tomy\",\"rank\":1}]}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction(
        "array",
        Seq(Literal("headmaster"), Literal("""{"name":"Tomy","rank":1}""")),
        isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")
    val jsonFunc =
      Alias(visit(JSON_APPEND, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_append() function: add single Object value") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_append('$validJson7',array('student', '{"name":"Tomy","rank":5}')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2},{\"name\":\"Tomy\",\"rank\":5}]}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction(
        "array",
        Seq(Literal("student"), Literal("""{"name":"Tomy","rank":5}""")),
        isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")
    val jsonFunc =
      Alias(visit(JSON_APPEND, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_append() function: add multi value") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_append('$validJson7', array('teacher', '"Tom"', 'teacher', '"Walt"')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"teacher\":[\"Alice\",\"Tom\",\"Walt\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction(
        "array",
        Seq(Literal("teacher"), Literal("\"Tom\""), Literal("teacher"), Literal("\"Walt\"")),
        isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")
    val jsonFunc =
      Alias(visit(JSON_APPEND, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_append() function: add nested array") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_append('$validJson7', array('teacher', '["Tom","Walt"]')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"teacher\":[\"Alice\",[\"Tom\",\"Walt\"]],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction(
        "array",
        Seq(Literal("teacher"), Literal("""["Tom","Walt"]""")),
        isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")
    val jsonFunc =
      Alias(visit(JSON_APPEND, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_append() function: add nested value") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_append('$validJson8', array('school.teacher', '"Tom"', 'school.teacher', '"Walt"')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"school\":{\"teacher\":[\"Alice\",\"Tom\",\"Walt\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction(
        "array",
        Seq(
          Literal("school.teacher"),
          Literal("\"Tom\""),
          Literal("school.teacher"),
          Literal("\"Walt\"")),
        isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"school\":{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}")
    val jsonFunc =
      Alias(visit(JSON_APPEND, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  // JSON_EXTEND

  test("test json_extend() function: add single value") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_extend('$validJson7',array('teacher', '"Tom"')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"teacher\":[\"Alice\",\"Tom\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction("array", Seq(Literal("teacher"), Literal("\"Tom\"")), isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")
    val jsonFunc =
      Alias(visit(JSON_EXTEND, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_extend() function: add single value key not found") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_extend('$validJson7',array('headmaster', 'Tom')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}],\"headmaster\":[\"Tom\"]}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction("array", Seq(Literal("headmaster"), Literal("Tom")), isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")
    val jsonFunc =
      Alias(visit(JSON_EXTEND, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_extend() function: add single Object key not found") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_extend('$validJson7',array('headmaster', '{"name":"Tomy","rank":1}')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}],\"headmaster\":[{\"name\":\"Tomy\",\"rank\":1}]}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction(
        "array",
        Seq(Literal("headmaster"), Literal("""{"name":"Tomy","rank":1}""")),
        isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")
    val jsonFunc =
      Alias(visit(JSON_EXTEND, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_extend() function: add single Object value") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_extend('$validJson7',array('student', '{"name":"Tomy","rank":5}')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2},{\"name\":\"Tomy\",\"rank\":5}]}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction(
        "array",
        Seq(Literal("student"), Literal("""{"name":"Tomy","rank":5}""")),
        isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")
    val jsonFunc =
      Alias(visit(JSON_EXTEND, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_extend() function: add multi value") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_extend('$validJson7', array('teacher', '"Tom"', 'teacher', '"Walt"')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"teacher\":[\"Alice\",\"Tom\",\"Walt\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction(
        "array",
        Seq(Literal("teacher"), Literal("\"Tom\""), Literal("teacher"), Literal("\"Walt\"")),
        isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")
    val jsonFunc =
      Alias(visit(JSON_EXTEND, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_extend() function: add nested array") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_extend('$validJson7', array('teacher', '["Tom","Walt"]')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"teacher\":[\"Alice\",\"Tom\",\"Walt\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction(
        "array",
        Seq(Literal("teacher"), Literal("""["Tom","Walt"]""")),
        isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}")
    val jsonFunc =
      Alias(visit(JSON_EXTEND, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test json_extend() function: add nested value") {
    val frame = sql(s"""
                       | source = $testTable
                       | | eval result = json_extend('$validJson8', array('school.teacher', '"Tom"', 'school.teacher', '"Walt"')) | head 1 | fields result
                       | """.stripMargin)
    assertSameRows(
      Seq(Row(
        "{\"school\":{\"teacher\":[\"Alice\",\"Tom\",\"Walt\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}")),
      frame)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val keysExpression =
      UnresolvedFunction(
        "array",
        Seq(
          Literal("school.teacher"),
          Literal("\"Tom\""),
          Literal("school.teacher"),
          Literal("\"Walt\"")),
        isDistinct = false)
    val jsonObjExp =
      Literal(
        "{\"school\":{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}")
    val jsonFunc =
      Alias(visit(JSON_EXTEND, util.List.of(jsonObjExp, keysExpression)), "result")()
    val eval = Project(Seq(UnresolvedStar(None), jsonFunc), table)
    val limit = GlobalLimit(Literal(1), LocalLimit(Literal(1), eval))
    val expectedPlan = Project(Seq(UnresolvedAttribute("result")), limit)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }
}
