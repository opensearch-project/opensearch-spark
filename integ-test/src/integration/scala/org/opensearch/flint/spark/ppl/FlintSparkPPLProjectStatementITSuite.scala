/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedIdentifier, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Descending, EqualTo, IsNotNull, Literal, Not, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.execution.command.{DescribeTableCommand, ExplainCommand}
import org.apache.spark.sql.streaming.StreamTest

import java.nio.file.{Files, Paths}

class FlintSparkPPLProjectStatementITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"
  private val t1 = "`spark_catalog`.`default`.`flint_ppl_test1`"
  private val t2 = "`spark_catalog`.default.`flint_ppl_test2`"
  private val t3 = "spark_catalog.`default`.`flint_ppl_test3`"
  private val t4 = "`spark_catalog`.`default`.flint_ppl_test4"
  private val viewName = "simpleView"
  // location of the projected view
  private val viewFolderLocation = Paths.get(".", "spark-warehouse", "student_partition_bucket")

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createPartitionedStateCountryTable(testTable)
    createPartitionedStateCountryTable(t1)
    createPartitionedStateCountryTable(t2)
    createPartitionedStateCountryTable(t3)
    createPartitionedStateCountryTable(t4)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    sql(s"DROP TABLE $viewName")
    // Delete the directory if it exists
    if (Files.exists(viewFolderLocation)) {
      Files.walk(viewFolderLocation)
        .sorted(java.util.Comparator.reverseOrder()) // Reverse order to delete files before directories
        .forEach(Files.delete)
    }
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  ignore("project sql test using csv") {
    val viewLocation = viewFolderLocation.toAbsolutePath.toString
    val frame = sql(s"""
                        | CREATE TABLE student_partition_bucket
                        |    USING parquet
                        |    OPTIONS (
                        |      'parquet.bloom.filter.enabled'='true',
                        |      'parquet.bloom.filter.enabled#age'='false'
                        |    )
                        |    PARTITIONED BY (age, country)
                        |    LOCATION '$viewLocation'
                        |    AS SELECT * FROM $testTable;
                        | """.stripMargin)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    frame.collect()
    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filter =
      Filter(Not(EqualTo(UnresolvedAttribute("state"), Literal("California"))), relation)
    val expectedPlan: LogicalPlan =
      ExplainCommand(
        Project(Seq(UnresolvedAttribute("name")), filter),
        ExplainMode.fromString("simple"))
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("project using csv") {
    val frame = sql(s"""
                       | project $viewName using csv | source = $testTable | where state != 'California' | fields name
                       | """.stripMargin)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    frame.collect()
    // verify new view was created correctly
    val results = sql(s"""
         | source = $viewName
         | """.stripMargin).collect()

    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Jane"), Row("John"), Row("Hello"))
    // Convert actual results to a Set for quick lookup
    val resultsSet: Set[Row] = results.toSet
    // Check that each expected row is present in the actual results
    expectedResults.foreach { expectedRow =>
      assert(resultsSet.contains(expectedRow), s"Expected row $expectedRow not found in results")
    }

    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filter =
      Filter(Not(EqualTo(UnresolvedAttribute("state"), Literal("California"))), relation)
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq(viewName)),
        Seq(),
        Project(Seq(UnresolvedAttribute("name")), filter),
        UnresolvedTableSpec(
          Map.empty,
          Option("CSV"),
          OptionList(Seq()),
          Option.empty,
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = false,
        isAnalyzed = false)
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("project using csv partition by age") {
    val frame = sql(s"""
                       | project $viewName using csv partitioned by (age) | source = $testTable | where state != 'California' | fields name, age
                       | """.stripMargin)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    frame.collect()
    // verify new view was created correctly
    val results = sql(s"""
         | source = $viewName
         | """.stripMargin).collect()

    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Jane", 20), Row("John", 25), Row("Hello", 30))
    // Convert actual results to a Set for quick lookup
    val resultsSet: Set[Row] = results.toSet
    // Check that each expected row is present in the actual results
    expectedResults.foreach { expectedRow =>
      assert(resultsSet.contains(expectedRow), s"Expected row $expectedRow not found in results")
    }

    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val filter =
      Filter(Not(EqualTo(UnresolvedAttribute("state"), Literal("California"))), relation)
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq(viewName)),
//      Seq(IdentityTransform.apply(FieldReference.apply("age"))),
        Seq(),
        Project(Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age")), filter),
        UnresolvedTableSpec(
          Map.empty,
          Option("CSV"),
          OptionList(Seq()),
          Option.empty,
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = false,
        isAnalyzed = false)
    // Compare the two plans
    assert(
      compareByString(logicalPlan) == expectedPlan.toString
    )
  }

  test("project using csv partition by state and country") {
    val frame = sql(s"""
                       |project $viewName using csv partitioned by (state, country) | source = $testTable | dedup name | fields name, state, country
                       | """.stripMargin)
    
    frame.collect()
    // verify new view was created correctly
    val results = sql(s"""
                         | source = $viewName
                         | """.stripMargin).collect()

    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Jane", "Quebec", "Canada"), Row("John", "Ontario", "Canada"), Row("Jake", "California", "USA"), Row("Hello", "New York", "USA"))
    // Convert actual results to a Set for quick lookup
    val resultsSet: Set[Row] = results.toSet
    // Check that each expected row is present in the actual results
    expectedResults.foreach { expectedRow =>
      assert(resultsSet.contains(expectedRow), s"Expected row $expectedRow not found in results")
    }

    // verify new view was created correctly
    val describe = sql(s"""
                         | describe $viewName
                         | """.stripMargin).collect()

    // Define the expected results
    val expectedDescribeResults: Array[Row] = Array(
      Row("Database", "default"),
      Row("Partition Provider", "Catalog"),
      Row("Type", "MANAGED"),
      Row("country", "string", "null"),
      Row("Catalog", "spark_catalog"),
      Row("state", "string", "null"),
      Row("# Partition Information", ""),
      Row("Created By", "Spark 3.5.1"),
      Row("Provider", "CSV"),
      Row("# Detailed Table Information", ""),
      Row("Table", "simpleview"),
      Row("Last Access", "UNKNOWN"),
      Row("# col_name", "data_type", "comment"),
      Row("name", "string", "null"))
    // Convert actual results to a Set for quick lookup
    val describeResults: Set[Row] = describe.toSet
    // Check that each expected row is present in the actual results
    expectedDescribeResults.foreach { expectedRow =>
      assert(expectedDescribeResults.contains(expectedRow), s"Expected row $expectedRow not found in results")
    }

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val nameAttribute = UnresolvedAttribute("name")
    val dedup =
      Deduplicate(Seq(nameAttribute), Filter(IsNotNull(nameAttribute), relation))
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq(viewName)),
        //      Seq(IdentityTransform.apply(FieldReference.apply("age")), IdentityTransform.apply(FieldReference.apply("state")),
        Seq(),
        Project(Seq(UnresolvedAttribute("name"), UnresolvedAttribute("state"), UnresolvedAttribute("country")), dedup),
        UnresolvedTableSpec(
          Map.empty,
          Option("CSV"),
          OptionList(Seq()),
          Option.empty,
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = false,
        isAnalyzed = false)
    // Compare the two plans
    assert(
      compareByString(logicalPlan) == expectedPlan.toString
    )
  }

  test("project using parquet partition by state & country") {
    val frame = sql(s"""
                       |project $viewName using parquet partitioned by (state, country) | source = $testTable | dedup name | fields name, state, country
                       | """.stripMargin)

    frame.collect()
    // verify new view was created correctly
    val results = sql(s"""
                         | source = $viewName
                         | """.stripMargin).collect()

    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Jane", "Quebec", "Canada"), Row("John", "Ontario", "Canada"), Row("Jake", "California", "USA"), Row("Hello", "New York", "USA"))
    // Convert actual results to a Set for quick lookup
    val resultsSet: Set[Row] = results.toSet
    // Check that each expected row is present in the actual results
    expectedResults.foreach { expectedRow =>
      assert(resultsSet.contains(expectedRow), s"Expected row $expectedRow not found in results")
    }

    // verify new view was created correctly
    val describe = sql(s"""
                          | describe $viewName
                          | """.stripMargin).collect()

    // Define the expected results
    val expectedDescribeResults: Array[Row] = Array(
      Row("Database", "default"),
      Row("Partition Provider", "Catalog"),
      Row("Type", "MANAGED"),
      Row("country", "string", "null"),
      Row("Catalog", "spark_catalog"),
      Row("state", "string", "null"),
      Row("# Partition Information", ""),
      Row("Created By", "Spark 3.5.1"),
      Row("Provider", "PARQUET"),
      Row("# Detailed Table Information", ""),
      Row("Table", "simpleview"),
      Row("Last Access", "UNKNOWN"),
      Row("# col_name", "data_type", "comment"),
      Row("name", "string", "null"))
    // Convert actual results to a Set for quick lookup
    val describeResults: Set[Row] = describe.toSet
    // Check that each expected row is present in the actual results
    expectedDescribeResults.foreach { expectedRow =>
      assert(expectedDescribeResults.contains(expectedRow), s"Expected row $expectedRow not found in results")
    }

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val nameAttribute = UnresolvedAttribute("name")
    val dedup =
      Deduplicate(Seq(nameAttribute), Filter(IsNotNull(nameAttribute), relation))
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq(viewName)),
        //      Seq(IdentityTransform.apply(FieldReference.apply("age")), IdentityTransform.apply(FieldReference.apply("state")),
        Seq(),
        Project(Seq(UnresolvedAttribute("name"), UnresolvedAttribute("state"), UnresolvedAttribute("country")), dedup),
        UnresolvedTableSpec(
          Map.empty,
          Option("PARQUET"),
          OptionList(Seq()),
          Option.empty,
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = false,
        isAnalyzed = false)
    // Compare the two plans
    assert(
      compareByString(logicalPlan) == expectedPlan.toString
    )
  }
  
  test("project using parquet with options & partition by state & country") {
    val frame = sql(s"""
                       | project $viewName using parquet OPTIONS('parquet.bloom.filter.enabled'='true', 'parquet.bloom.filter.enabled#age'='false')
                       | partitioned by (state, country) | source = $testTable | dedup name | fields name, state, country
                       | """.stripMargin)

    frame.collect()
    // verify new view was created correctly
    val results = sql(s"""
                         | source = $viewName
                         | """.stripMargin).collect()

    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Jane", "Quebec", "Canada"), Row("John", "Ontario", "Canada"), Row("Jake", "California", "USA"), Row("Hello", "New York", "USA"))
    // Convert actual results to a Set for quick lookup
    val resultsSet: Set[Row] = results.toSet
    // Check that each expected row is present in the actual results
    expectedResults.foreach { expectedRow =>
      assert(resultsSet.contains(expectedRow), s"Expected row $expectedRow not found in results")
    }

    // verify new view was created correctly
    val describe = sql(s"""
                          | describe $viewName
                          | """.stripMargin).collect()

    // Define the expected results
    val expectedDescribeResults: Array[Row] = Array(
      Row("Database", "default"),
      Row("Partition Provider", "Catalog"),
      Row("Type", "MANAGED"),
      Row("country", "string", "null"),
      Row("Catalog", "spark_catalog"),
      Row("state", "string", "null"),
      Row("# Partition Information", ""),
      Row("Created By", "Spark 3.5.1"),
      Row("Provider", "PARQUET"),
      Row("# Detailed Table Information", ""),
      Row("Table", "simpleview"),
      Row("Last Access", "UNKNOWN"),
      Row("# col_name", "data_type", "comment"),
      Row("name", "string", "null"))
    // Convert actual results to a Set for quick lookup
    val describeResults: Set[Row] = describe.toSet
    // Check that each expected row is present in the actual results
    expectedDescribeResults.foreach { expectedRow =>
      assert(expectedDescribeResults.contains(expectedRow), s"Expected row $expectedRow not found in results")
    }

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val nameAttribute = UnresolvedAttribute("name")
    val dedup =
      Deduplicate(Seq(nameAttribute), Filter(IsNotNull(nameAttribute), relation))
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq(viewName)),
        //      Seq(IdentityTransform.apply(FieldReference.apply("age")), IdentityTransform.apply(FieldReference.apply("state")),
        Seq(),
        Project(Seq(UnresolvedAttribute("name"), UnresolvedAttribute("state"), UnresolvedAttribute("country")), dedup),
        UnresolvedTableSpec(
          Map.empty,
          Option("PARQUET"),
          OptionList(Seq(
            ("parquet.bloom.filter.enabled", Literal("true")),
            ("parquet.bloom.filter.enabled#age", Literal("false")))
          ),
          Option.empty,
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = false,
        isAnalyzed = false)
    // Compare the two plans
    assert(
      compareByString(logicalPlan) == expectedPlan.toString
    )
  }
  
  test("project using parquet with options & location with partition by state & country") {
    val viewLocation = viewFolderLocation.toAbsolutePath.toString
    val frame = sql(s"""
                       | project $viewName using parquet OPTIONS('parquet.bloom.filter.enabled'='true', 'parquet.bloom.filter.enabled#age'='false')
                       | partitioned by (state, country) location '$viewLocation' | source = $testTable | dedup name | fields name, state, country
                       | """.stripMargin)

    frame.collect()
    // verify new view was created correctly
    val results = sql(s"""
                         | source = $viewName
                         | """.stripMargin).collect()

    // Define the expected results
    val expectedResults: Array[Row] = Array(Row("Jane", "Quebec", "Canada"), Row("John", "Ontario", "Canada"), Row("Jake", "California", "USA"), Row("Hello", "New York", "USA"))
    // Convert actual results to a Set for quick lookup
    val resultsSet: Set[Row] = results.toSet
    // Check that each expected row is present in the actual results
    expectedResults.foreach { expectedRow =>
      assert(resultsSet.contains(expectedRow), s"Expected row $expectedRow not found in results")
    }

    // verify new view was created correctly
    val describe = sql(s"""
                          | describe $viewName
                          | """.stripMargin).collect()

    // Define the expected results
    val expectedDescribeResults: Array[Row] = Array(
      Row("Database", "default"),
      Row("Partition Provider", "Catalog"),
      Row("Type", "MANAGED"),
      Row("country", "string", "null"),
      Row("Catalog", "spark_catalog"),
      Row("state", "string", "null"),
      Row("# Partition Information", ""),
      Row("Created By", "Spark 3.5.1"),
      Row("Provider", "PARQUET"),
      Row("# Detailed Table Information", ""),
      Row("Table", "simpleview"),
      Row("Last Access", "UNKNOWN"),
      Row("# col_name", "data_type", "comment"),
      Row("name", "string", "null"))
    // Convert actual results to a Set for quick lookup
    val describeResults: Set[Row] = describe.toSet
    // Check that each expected row is present in the actual results
    expectedDescribeResults.foreach { expectedRow =>
      assert(expectedDescribeResults.contains(expectedRow), s"Expected row $expectedRow not found in results")
    }

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val nameAttribute = UnresolvedAttribute("name")
    val dedup =
      Deduplicate(Seq(nameAttribute), Filter(IsNotNull(nameAttribute), relation))
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq(viewName)),
        //      Seq(IdentityTransform.apply(FieldReference.apply("age")), IdentityTransform.apply(FieldReference.apply("state")),
        Seq(),
        Project(Seq(UnresolvedAttribute("name"), UnresolvedAttribute("state"), UnresolvedAttribute("country")), dedup),
        UnresolvedTableSpec(
          Map.empty,
          Option("PARQUET"),
          OptionList(Seq(
            ("parquet.bloom.filter.enabled", Literal("true")),
            ("parquet.bloom.filter.enabled#age", Literal("false")))
          ),
          Option(viewLocation),
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = false,
        isAnalyzed = false)
    // Compare the two plans
    assert(
      compareByString(logicalPlan) == expectedPlan.toString
    )
  }

}
