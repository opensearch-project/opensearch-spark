/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Coalesce, Descending, GreaterThan, Literal, NullsLast, RegExpExtract, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLGrokITSuite
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
  
  test("test grok email expressions parsing") {
    val frame = sql(s"""
         | source = $testTable| grok email '.+@%{HOSTNAME:host}' | fields email, host
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
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
    val hostExpression = Alias(
      RegExpExtract(
        emailAttribute,
        Literal(
          ".+@(?<name0>\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b))"),
        Literal("1")),
      "host")()
    val expectedPlan = Project(
      Seq(emailAttribute, hostAttribute),
      Project(
        Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))))
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("test parse email expressions parsing filter & sort by age") {
    val frame = sql(s"""
         | source = $testTable| grok email '.+@%{HOSTNAME:host}' | where age > 45 | sort - age | fields age, email, host ;
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
    val hostExpression = Alias(
      RegExpExtract(
        emailAttribute,
        Literal(
          ".+@(?<name0>\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b))"),
        Literal(1)),
      "host")()

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
         | source = $testTable| grok email '.+@%{HOSTNAME:host}'  | stats count() by host
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
    val hostExpression = Alias(
      RegExpExtract(
        emailAttribute,
        Literal(
          ".+@(?<name0>\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b))"),
        Literal(1)),
      "host")()

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
         | source = $testTable| grok email '.+@%{HOSTNAME:host}'  | top 1 host
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
    val hostExpression = Alias(
      RegExpExtract(
        emailAttribute,
        Literal(
          ".+@(?<name0>\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b))"),
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

  ignore("test grok to grok raw logs.") {
    val frame = sql(s"""
                  | source= $testTable | grok message '%{COMMONAPACHELOG}' | fields COMMONAPACHELOG, timestamp, response, bytes
                  | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
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

    val messageAttribute = UnresolvedAttribute("message")
    val logAttribute = UnresolvedAttribute("COMMONAPACHELOG")
    val timestampAttribute = UnresolvedAttribute("timestamp")
    val responseAttribute = UnresolvedAttribute("response")
    val bytesAttribute = UnresolvedAttribute("bytes")
    // scalastyle:off
    val expectedRegExp =
      "(?<name0>(?<name1>(?:(?<name2>\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b))|(?<name3>(?:(?<name4>((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,4}|((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){5}(((:[0-9A-Fa-f]{1,4}){1,2})|:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){4}(((:[0-9A-Fa-f]{1,4}){1,3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,2}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4}){1,5})|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){1}(((:[0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,5}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:)))(%.+)?)|(?<name5>(?<![0-9])(?:(?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2}))(?![0-9])))))) (?<name6>(?<name7>[a-zA-Z0-9._-]+)) (?<name8>(?<name9>[a-zA-Z0-9._-]+)) \\[(?<name10>(?<name11>(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9]))/(?<name12>\\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\\b)/(?<name13>(?>\\d\\d){1,2}):(?<name14>(?!<[0-9])(?<name15>(?:2[0123]|[01]?[0-9])):(?<name16>(?:[0-5][0-9]))(?::(?<name17>(?:(?:[0-5]?[0-9]|60)(?:[:.,][0-9]+)?)))(?![0-9])) (?<name18>(?:[+-]?(?:[0-9]+))))\\] \"(?:(?<name19>\\b\\w+\\b) (?<name20>\\S+)(?: HTTP/(?<name21>(?:(?<name22>(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))))))?|(?<name23>.*?))\" (?<name24>(?:(?<name25>(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))))) (?:(?<name26>(?:(?<name27>(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+))))))|-))"
    // scalastyle:on

    val COMMONAPACHELOG = Alias(
      RegExpExtract(messageAttribute, Literal(expectedRegExp), Literal("1")),
      "COMMONAPACHELOG")()
    val timestamp =
      Alias(RegExpExtract(messageAttribute, Literal(expectedRegExp), Literal("5")), "timestamp")()
    val response =
      Alias(RegExpExtract(messageAttribute, Literal(expectedRegExp), Literal("18")), "response")()
    val bytes =
      Alias(RegExpExtract(messageAttribute, Literal(expectedRegExp), Literal("19")), "bytes")()
    val expectedPlan = Project(
      Seq(logAttribute, timestampAttribute, responseAttribute, bytesAttribute),
      Project(
        Seq(messageAttribute, COMMONAPACHELOG, timestamp, response, bytes, UnresolvedStar(None)),
        UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))))
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("test grok address expressions with 2 fields identifies ") {
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
