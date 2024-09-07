/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Descending, GreaterThan, Literal, NullsLast, RegExpExtract, RegExpReplace, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.{QueryTest, Row}

class FlintSparkPPLPatternsITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createPartitionedGrokEmailTable(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test patterns email & host expressions") {
    val frame = sql(s"""
         | source = $testTable| patterns email | fields email, patterns_field
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("charlie@domain.net", "@."),
      Row("david@anotherdomain.com", "@."),
      Row("hank@demonstration.com", "@."),
      Row("alice@example.com", "@."),
      Row("frank@sample.org", "@."),
      Row("grace@demo.net", "@."),
      Row("jack@sample.net", "@."),
      Row("eve@examples.com", "@."),
      Row("ivy@examples.com", "@."),
      Row("bob@test.org", "@."))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val emailAttribute = UnresolvedAttribute("email")
    val patterns_field = UnresolvedAttribute("patterns_field")
    val hostExpression = Alias(
      RegExpReplace(
        emailAttribute,
        Literal(
          "[a-zA-Z0-9]"),
        Literal("")),
      "patterns_field")()
    val expectedPlan = Project(
      Seq(emailAttribute, patterns_field),
      Project(
        Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))))
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("test patterns email expressions parsing filter & sort by age") {
    val frame = sql(s"""
         | source = $testTable| patterns email | where age > 45 | sort - age | fields age, email, patterns_field;
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row(76, "frank@sample.org", "@."),
      Row(65, "charlie@domain.net", "@."),
      Row(55, "bob@test.org", "@."))

    // Compare the results
    assert(results.sameElements(expectedResults))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val emailAttribute = UnresolvedAttribute("email")
    val patterns_fieldAttribute = UnresolvedAttribute("patterns_field")
    val ageAttribute = UnresolvedAttribute("age")
    val patternExpression = Alias(
      RegExpReplace(
        emailAttribute,
        Literal(
          "[a-zA-Z0-9]"),
        Literal("")),
      "patterns_field")()

    // Define the corrected expected plan
    val expectedPlan = Project(
      Seq(ageAttribute, emailAttribute, patterns_fieldAttribute),
      Sort(
        Seq(SortOrder(ageAttribute, Descending, NullsLast, Seq.empty)),
        global = true,
        Filter(
          GreaterThan(ageAttribute, Literal(45)),
          Project(
            Seq(emailAttribute, patternExpression, UnresolvedStar(None)),
            UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))))))
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("test patterns email expressions and top count_host ") {
    val frame = sql("source=spark_catalog.default.flint_ppl_test | patterns new_field='dot_com' pattern='(.com|.net|.org)' email | stats count() by dot_com ")

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row(1L, "charlie@domain"),
      Row(1L, "david@anotherdomain"),
      Row(1L, "hank@demonstration"),
      Row(1L, "alice@example"),
      Row(1L, "frank@sample"),
      Row(1L, "grace@demo"),
      Row(1L, "jack@sample"),
      Row(1L, "eve@examples"),
      Row(1L, "ivy@examples"),
      Row(1L, "bob@test"))

    // Sort both the results and the expected results
    implicit val rowOrdering: Ordering[Row] = Ordering.by(r => (r.getLong(0), r.getString(1)))
    assert(results.sorted.sameElements(expectedResults.sorted))
    
    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val messageAttribute = UnresolvedAttribute("email")
    val noNumbersAttribute = UnresolvedAttribute("dot_com")
    val hostExpression = Alias(
      RegExpReplace(
        messageAttribute,
        Literal(
          "(.com|.net|.org)"),
        Literal("")),
      "dot_com")()

    // Define the corrected expected plan
    val expectedPlan = Project(
      Seq(UnresolvedStar(None)), // Matches the '*' in the Project
      Aggregate(
        Seq(Alias(noNumbersAttribute, "dot_com")()), // Group by 'no_numbers'
        Seq(
          Alias(
            UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
            "count()")(),
          Alias(noNumbersAttribute, "dot_com")()),
        Project(
          Seq(messageAttribute, hostExpression, UnresolvedStar(None)),
          UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))))

    // Compare the logical plans
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }
}
