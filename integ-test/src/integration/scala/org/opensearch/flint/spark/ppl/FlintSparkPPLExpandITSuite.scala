/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import java.nio.file.Files

import scala.collection.mutable

import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo, Explode, GeneratorOuter, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLExpandITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  private val testTable = "flint_ppl_test"
  private val occupationTable = "spark_catalog.default.flint_ppl_flat_table_test"
  private val structNestedTable = "spark_catalog.default.flint_ppl_struct_nested_test"
  private val structTable = "spark_catalog.default.flint_ppl_struct_test"
  private val multiValueTable = "spark_catalog.default.flint_ppl_multi_value_test"
  private val tempFile = Files.createTempFile("jsonTestData", ".json")

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createNestedJsonContentTable(tempFile, testTable)
    createOccupationTable(occupationTable)
    createStructNestedTable(structNestedTable)
    createStructTable(structTable)
    createMultiValueStructTable(multiValueTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    Files.deleteIfExists(tempFile)
  }

  test("expand for eval field of an array") {
    val frame = sql(
      s""" source = $occupationTable | eval array=json_array(1, 2, 3) | expand array as uid | fields name, occupation, uid
       """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row("Jake", "Engineer", 1),
      Row("Jake", "Engineer", 2),
      Row("Jake", "Engineer", 3),
      Row("Hello", "Artist", 1),
      Row("Hello", "Artist", 2),
      Row("Hello", "Artist", 3),
      Row("John", "Doctor", 1),
      Row("John", "Doctor", 2),
      Row("John", "Doctor", 3),
      Row("David", "Doctor", 1),
      Row("David", "Doctor", 2),
      Row("David", "Doctor", 3),
      Row("David", "Unemployed", 1),
      Row("David", "Unemployed", 2),
      Row("David", "Unemployed", 3),
      Row("Jane", "Scientist", 1),
      Row("Jane", "Scientist", 2),
      Row("Jane", "Scientist", 3))

    // Compare the results
    assert(results.toSet == expectedResults.toSet)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // expected plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_flat_table_test"))
    val jsonFunc =
      UnresolvedFunction("array", Seq(Literal(1), Literal(2), Literal(3)), isDistinct = false)
    val aliasA = Alias(jsonFunc, "array")()
    val project = Project(seq(UnresolvedStar(None), aliasA), table)
    val generate = Generate(
      Explode(UnresolvedAttribute("array")),
      seq(),
      false,
      None,
      seq(UnresolvedAttribute("uid")),
      project)
    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("array")), generate)
    val expectedPlan = Project(
      seq(
        UnresolvedAttribute("name"),
        UnresolvedAttribute("occupation"),
        UnresolvedAttribute("uid")),
      dropSourceColumn)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("expand for structs") {
    val frame = sql(
      s""" source = $multiValueTable | expand multi_value AS exploded_multi_value | fields exploded_multi_value
       """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(Row("1_one", 1)),
      Row(Row(null, 11)),
      Row(Row("1_three", null)),
      Row(Row("2_Monday", 2)),
      Row(null),
      Row(Row("3_third", 3)),
      Row(Row("3_4th", 4)),
      Row(null))
    // Compare the results
    assert(results.toSet == expectedResults.toSet)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // expected plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_multi_value_test"))
    val generate = Generate(
      Explode(UnresolvedAttribute("multi_value")),
      seq(),
      outer = false,
      None,
      seq(UnresolvedAttribute("exploded_multi_value")),
      table)
    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("multi_value")), generate)
    val expectedPlan = Project(Seq(UnresolvedAttribute("exploded_multi_value")), dropSourceColumn)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("expand for array of structs") {
    val frame = sql(s"""
                       | source = $testTable
                       | | where country = 'England' or country = 'Poland'
                       | | expand bridges 
                       | | fields bridges
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(mutable.WrappedArray.make(Array(Row(801, "Tower Bridge"), Row(928, "London Bridge")))),
      Row(mutable.WrappedArray.make(Array(Row(801, "Tower Bridge"), Row(928, "London Bridge"))))
      // Row(null)) -> in case of outerGenerator = GeneratorOuter(Explode(UnresolvedAttribute("bridges"))) it will include the `null` row
    )

    // Compare the results
    assert(results.toSet == expectedResults.toSet)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("flint_ppl_test"))
    val filter = Filter(
      Or(
        EqualTo(UnresolvedAttribute("country"), Literal("England")),
        EqualTo(UnresolvedAttribute("country"), Literal("Poland"))),
      table)
    val generate =
      Generate(Explode(UnresolvedAttribute("bridges")), seq(), outer = false, None, seq(), filter)
    val expectedPlan = Project(Seq(UnresolvedAttribute("bridges")), generate)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("expand for array of structs with alias") {
    val frame = sql(s"""
                       | source = $testTable
                       | | where country = 'England' 
                       | | expand bridges as britishBridges
                       | | fields britishBridges
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] = Array(
      Row(Row(801, "Tower Bridge")),
      Row(Row(928, "London Bridge")),
      Row(Row(801, "Tower Bridge")),
      Row(Row(928, "London Bridge")))
    // Compare the results
    assert(results.toSet == expectedResults.toSet)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("flint_ppl_test"))
    val filter = Filter(EqualTo(UnresolvedAttribute("country"), Literal("England")), table)
    val generate = Generate(
      Explode(UnresolvedAttribute("bridges")),
      seq(),
      outer = false,
      None,
      seq(UnresolvedAttribute("britishBridges")),
      filter)
    val dropColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("bridges")), generate)
    val expectedPlan = Project(Seq(UnresolvedAttribute("britishBridges")), dropColumn)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  ignore("expand struct table") {
    val frame = sql(s"""
         | source = $structTable
         | | expand struct_col
         | | expand field1
         | """.stripMargin)

    assert(frame.columns.sameElements(Array("int_col", "field2", "subfield")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(Row(30, 123, "value1"), Row(40, 456, "value2"), Row(50, 789, "value3"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_struct_test"))
    val generate = Generate(
      Explode(UnresolvedAttribute("bridges")),
      seq(),
      outer = false,
      None,
      seq(UnresolvedAttribute("britishBridges")),
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), generate)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  ignore("expand struct nested table") {
    val frame = sql(s"""
                       | source = $structNestedTable
                       | | expand struct_col
                       | | expand field1
                       | | expand struct_col2
                       | | expand field1
                       | """.stripMargin)

    assert(
      frame.columns.sameElements(Array("int_col", "field2", "subfield", "field2", "subfield")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(30, 123, "value1", 23, "valueA"),
        Row(40, 123, "value5", 33, "valueB"),
        Row(30, 823, "value4", 83, "valueC"),
        Row(40, 456, "value2", 46, "valueD"),
        Row(50, 789, "value3", 89, "valueE"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table =
      UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_struct_nested_test"))
//    val expandStructCol = generator("struct_col", table)
//    val expandField1 = generator("field1", expandStructCol)
//    val expandStructCol2 = generator("struct_col2", expandField1)
//    val expandField1Again = generator("field1", expandStructCol2)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), table)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  ignore("expand multi value nullable") {
    val frame = sql(s"""
                       | source = $multiValueTable
                       | | expand multi_value as expand_field
                       | | fields expand_field
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("expand_field")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(1, "1_one", 1),
        Row(1, null, 11),
        Row(1, "1_three", null),
        Row(2, "2_Monday", 2),
        Row(2, null, null),
        Row(3, "3_third", 3),
        Row(3, "3_4th", 4),
        Row(4, null, null))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_multi_value_test"))
    val generate = Generate(
      Explode(UnresolvedAttribute("bridges")),
      seq(),
      outer = false,
      None,
      seq(UnresolvedAttribute("britishBridges")),
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), generate)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }
}
