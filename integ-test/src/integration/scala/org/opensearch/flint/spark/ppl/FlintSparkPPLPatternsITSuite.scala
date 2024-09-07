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
    val frame = sql("source=spark_catalog.default.flint_ppl_test | patterns new_field='dot_com' pattern='[0-9]' | top 1 dot_com")

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(2L, ".com"))

    // Sort both the results and the expected results
    implicit val rowOrdering: Ordering[Row] = Ordering.by(r => (r.getLong(0), r.getString(1)))
    assert(results.sorted.sameElements(expectedResults.sorted))
    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val hostExpression = Alias(
      RegExpExtract(
        emailAttribute,
        Literal(
          ".*(\\.com)$"),
        Literal(1)),
      "host")()

    val sortedPlan = Sort(
      Seq(
        SortOrder(
          Alias(
            UnresolvedFunction(Seq("COUNT"), Seq(hostAttribute), isDistinct = false),
            "count_host")(),
          Descending,
          NullsLast,
          Seq.empty)),
      global = true,
      Aggregate(
        Seq(hostAttribute),
        Seq(
          Alias(
            UnresolvedFunction(Seq("COUNT"), Seq(hostAttribute), isDistinct = false),
            "count_host")(),
          hostAttribute),
        Project(
          Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
          UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))))
    // Define the corrected expected plan
    val expectedPlan = Project(
      Seq(UnresolvedStar(None)), // Matches the '*' in the Project
      GlobalLimit(Literal(1), LocalLimit(Literal(1), sortedPlan)))
    // Compare the logical plans
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }
  
  test("test patterns address expressions with 2 fields identifies ") {
    val frame = sql(s"""
                | source= $testTable | grok street_address '%{NUMBER} %{GREEDYDATA:address}' | fields address
                | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Pine St, San Francisco"),
      Row("Maple St, New York"),
      Row("Spruce St, Miami"),
      Row("Main St, Seattle"),
      Row("Cedar St, Austin"),
      Row("Birch St, Chicago"),
      Row("Ash St, Seattle"),
      Row("Oak St, Boston"),
      Row("Fir St, Denver"),
      Row("Elm St, Portland"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val street_addressAttribute = UnresolvedAttribute("street_address")
    val addressAttribute = UnresolvedAttribute("address")
    val addressExpression = Alias(
      RegExpExtract(
        street_addressAttribute,
        Literal(
          "(?<name0>(?:(?<name1>(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))))) (?<name2>.*)"),
        Literal("3")),
      "address")()
    val expectedPlan = Project(
      Seq(addressAttribute),
      Project(
        Seq(street_addressAttribute, addressExpression, UnresolvedStar(None)),
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))))
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

}
