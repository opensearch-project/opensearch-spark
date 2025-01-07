/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Cast, Descending, GreaterThan, Literal, NullsFirst, NullsLast, RegExpExtract, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, GlobalLimit, LocalLimit, LogicalPlan, Project, Sort}
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.IntegerType

class FlintSparkPPLParseITSuite
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

  test("test parse email expressions parsing") {
    val frame = sql(s"""
         | source = $testTable| parse email '.+@(?<host>.+)' | fields email, host ;
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("charlie@domain.net", "domain.net"),
      Row("david@anotherdomain.com", "anotherdomain.com"),
      Row("hank@demonstration.com", "demonstration.com"),
      Row("alice@example.com", "example.com"),
      Row("frank@sample.org", "sample.org"),
      Row("grace@demo.net", "demo.net"),
      Row("jack@sample.net", "sample.net"),
      Row("eve@examples.com", "examples.com"),
      Row("ivy@examples.com", "examples.com"),
      Row("bob@test.org", "test.org"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val hostExpression =
      Alias(RegExpExtract(emailAttribute, Literal(".+@(?<host>.+)"), Literal("1")), "host")()
    val expectedPlan = Project(
      Seq(emailAttribute, hostAttribute),
      Project(
        Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))))
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("test parse email expressions parsing filter & sort by age") {
    val frame = sql(s"""
         | source = $testTable| parse email '.+@(?<host>.+)' | where age > 45 | sort - age | fields age, email, host ;
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row(76, "frank@sample.org", "sample.org"),
      Row(65, "charlie@domain.net", "domain.net"),
      Row(55, "bob@test.org", "test.org"))

    // Compare the results
    assert(results.sameElements(expectedResults))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val emailAttribute = UnresolvedAttribute("email")
    val ageAttribute = UnresolvedAttribute("age")
    val hostExpression =
      Alias(RegExpExtract(emailAttribute, Literal(".+@(?<host>.+)"), Literal(1)), "host")()

    // Define the corrected expected plan
    val expectedPlan = Project(
      Seq(ageAttribute, emailAttribute, UnresolvedAttribute("host")),
      Sort(
        Seq(SortOrder(ageAttribute, Descending, NullsLast, Seq.empty)),
        global = true,
        Filter(
          GreaterThan(ageAttribute, Literal(45)),
          Project(
            Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
            UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))))))
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test parse email expressions and group by count host ") {
    val frame = sql(s"""
         | source = $testTable| parse email '.+@(?<host>.+)' | stats count() by host
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row(1L, "demonstration.com"),
      Row(1L, "example.com"),
      Row(1L, "domain.net"),
      Row(1L, "anotherdomain.com"),
      Row(1L, "sample.org"),
      Row(1L, "demo.net"),
      Row(1L, "sample.net"),
      Row(2L, "examples.com"),
      Row(1L, "test.org"))

    // Sort both the results and the expected results
    implicit val rowOrdering: Ordering[Row] = Ordering.by(r => (r.getLong(0), r.getString(1)))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val hostExpression =
      Alias(RegExpExtract(emailAttribute, Literal(".+@(?<host>.+)"), Literal(1)), "host")()

    // Define the corrected expected plan
    val expectedPlan = Project(
      Seq(UnresolvedStar(None)), // Matches the '*' in the Project
      Aggregate(
        Seq(Alias(hostAttribute, "host")()), // Group by 'host'
        Seq(
          Alias(
            UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
            "count()")(),
          Alias(hostAttribute, "host")()),
        Project(
          Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
          UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))))
    // Compare the logical plans
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

  test("test parse email expressions and top count_host ") {
    val frame = sql(s"""
         | source = $testTable| parse email '.+@(?<host>.+)' | top 1 host
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(Row(2L, "examples.com"))

    // Sort both the results and the expected results
    implicit val rowOrdering: Ordering[Row] = Ordering.by(r => (r.getLong(0), r.getString(1)))
    assert(results.sorted.sameElements(expectedResults.sorted))
    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val hostExpression =
      Alias(RegExpExtract(emailAttribute, Literal(".+@(?<host>.+)"), Literal(1)), "host")()

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

  test("test parse street number & street expressions including cast and sort commands") {

    // TODO #963: Implement 'num', 'str', and 'ip' sort syntax
    val query = s"source = $testTable | " +
      "parse street_address '(?<streetNumber>\\d+) (?<street>.+)' | " +
      "eval streetNumberInt = cast(streetNumber as integer) | " +
      "where streetNumberInt > 500 | " +
      "sort streetNumberInt | " +
      "fields streetNumber, street"
    val frame = sql(query)
    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("505", "Spruce St, Miami"),
      Row("606", "Fir St, Denver"),
      Row("707", "Ash St, Seattle"),
      Row("789", "Pine St, San Francisco"))

    // Sort both the results and the expected results
    implicit val rowOrdering: Ordering[Row] = Ordering.by(r => (r.getString(0), r.getString(1)))
    assert(results.sorted.sameElements(expectedResults.sorted))
    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val streetAddressAttribute = UnresolvedAttribute("street_address")
    val streetNumberAttribute = UnresolvedAttribute("streetNumber")
    val streetAttribute = UnresolvedAttribute("street")
    val streetNumberIntAttribute = UnresolvedAttribute("streetNumberInt")

    val regexLiteral = Literal("(?<streetNumber>\\d+) (?<street>.+)")
    val streetNumberExpression =
      Alias(RegExpExtract(streetAddressAttribute, regexLiteral, Literal("1")), "streetNumber")()
    val streetExpression =
      Alias(RegExpExtract(streetAddressAttribute, regexLiteral, Literal("2")), "street")()

    val castExpression = Cast(streetNumberAttribute, IntegerType)

    val expectedPlan = Project(
      Seq(streetNumberAttribute, streetAttribute),
      Sort(
        Seq(SortOrder(streetNumberIntAttribute, Ascending, NullsFirst, Seq.empty)),
        global = true,
        Filter(
          GreaterThan(streetNumberIntAttribute, Literal(500)),
          Project(
            Seq(UnresolvedStar(None), Alias(castExpression, "streetNumberInt")()),
            Project(
              Seq(
                streetAddressAttribute,
                streetNumberExpression,
                streetExpression,
                UnresolvedStar(None)),
              UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))))))

    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }
}
