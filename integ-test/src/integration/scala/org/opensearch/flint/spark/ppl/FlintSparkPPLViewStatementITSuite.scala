/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedIdentifier, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Descending, Divide, EqualTo, Floor, GreaterThan, IsNotNull, Literal, Multiply, Not, Or, SortOrder}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, NamedReference, Transform}
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.execution.command.{DescribeTableCommand, ExplainCommand}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLViewStatementITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table */
  private val testTable = "spark_catalog.default.flint_ppl_test"
  private val testTable1 = "spark_catalog.default.flint_ppl_test1"
  private val testTable2 = "spark_catalog.default.flint_ppl_test2"

  private val t1 = "`spark_catalog`.`default`.`flint_ppl_t1`"
  private val t2 = "`spark_catalog`.default.`flint_ppl_t2`"

  /* view projection */
  private val viewName = "simpleView"
  // location of the projected view
  private val viewFolderLocation = Paths.get(".", "spark-warehouse", "student_partition_bucket")

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createPartitionedStateCountryTable(testTable1)
    // Update data insertion
    sql(s"""
           | INSERT INTO $testTable1
           | PARTITION (year=2023, month=4)
           | VALUES ('Jim', 27,  'B.C', 'Canada'),
           |        ('Peter', 57,  'B.C', 'Canada'),
           |        ('Rick', 70,  'B.C', 'Canada'),
           |        ('David', 40,  'Washington', 'USA')
           | """.stripMargin)

    createOccupationTable(testTable2)
    // none join tables
    createPartitionedStateCountryTable(testTable)
    createPartitionedStateCountryTable(t1)
    createPartitionedStateCountryTable(t2)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    sql(s"DROP TABLE $viewName")
    // Delete the directory if it exists
    if (Files.exists(viewFolderLocation)) {
      Files
        .walk(viewFolderLocation)
        .sorted(
          java.util.Comparator.reverseOrder()
        ) // Reverse order to delete files before directories
        .forEach(Files.delete)
    }
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  ignore("view sql test using csv") {
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

  test("view using csv") {
    val frame = sql(s"""
                       | view $viewName using csv | source = $testTable | where state != 'California' | fields name
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
    comparePlans(
      logicalPlan.asInstanceOf[CreateTableAsSelect].query,
      expectedPlan.asInstanceOf[CreateTableAsSelect].query,
      checkAnalysis = false)
    comparePlans(
      logicalPlan.asInstanceOf[CreateTableAsSelect].name,
      expectedPlan.asInstanceOf[CreateTableAsSelect].name,
      checkAnalysis = false)
    assert(
      logicalPlan.asInstanceOf[CreateTableAsSelect].tableSpec.toString == expectedPlan
        .asInstanceOf[CreateTableAsSelect]
        .tableSpec
        .toString)
    assert(
      logicalPlan.asInstanceOf[CreateTableAsSelect].partitioning.isEmpty,
      "Partitioning does not contain ay FieldReferences")
  }

  test("view using csv partition by age") {
    val frame = sql(s"""
                       | view $viewName using csv partitioned by (age) | source = $testTable | where state != 'California' | fields name, age
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
    comparePlans(
      logicalPlan.asInstanceOf[CreateTableAsSelect].query,
      expectedPlan.asInstanceOf[CreateTableAsSelect].query,
      checkAnalysis = false)
    comparePlans(
      logicalPlan.asInstanceOf[CreateTableAsSelect].name,
      expectedPlan.asInstanceOf[CreateTableAsSelect].name,
      checkAnalysis = false)
    assert(
      logicalPlan.asInstanceOf[CreateTableAsSelect].tableSpec.toString == expectedPlan
        .asInstanceOf[CreateTableAsSelect]
        .tableSpec
        .toString)
    assert(
      logicalPlan.asInstanceOf[CreateTableAsSelect].partitioning.exists {
        case transform: Transform =>
          transform.arguments().exists {
            case fieldRef: NamedReference => fieldRef.fieldNames().contains("age")
            case _ => false
          }
        case _ => false
      } && logicalPlan.asInstanceOf[CreateTableAsSelect].partitioning.length == 1,
      "Partitioning does not contain a FieldReferences: 'age'")
  }

  test("view using csv partition by state and country") {
    val frame = sql(s"""
                       |view $viewName using csv partitioned by (state, country) | source = $testTable | dedup name | fields name, state, country
                       | """.stripMargin)

    frame.collect()
    // verify new view was created correctly
    val results = sql(s"""
                         | source = $viewName
                         | """.stripMargin).collect()

    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jane", "Quebec", "Canada"),
      Row("John", "Ontario", "Canada"),
      Row("Jake", "California", "USA"),
      Row("Hello", "New York", "USA"))
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
      assert(
        expectedDescribeResults.contains(expectedRow),
        s"Expected row $expectedRow not found in results")
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
        Project(
          Seq(
            UnresolvedAttribute("name"),
            UnresolvedAttribute("state"),
            UnresolvedAttribute("country")),
          dedup),
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
    comparePlans(
      logicalPlan.asInstanceOf[CreateTableAsSelect].query,
      expectedPlan.asInstanceOf[CreateTableAsSelect].query,
      checkAnalysis = false)
    comparePlans(
      logicalPlan.asInstanceOf[CreateTableAsSelect].name,
      expectedPlan.asInstanceOf[CreateTableAsSelect].name,
      checkAnalysis = false)
    assert(
      logicalPlan.asInstanceOf[CreateTableAsSelect].tableSpec.toString == expectedPlan
        .asInstanceOf[CreateTableAsSelect]
        .tableSpec
        .toString)
    assert(
      logicalPlan.asInstanceOf[CreateTableAsSelect].partitioning.exists {
        case transform: Transform =>
          transform.arguments().exists {
            case fieldRef: NamedReference =>
              fieldRef.fieldNames().contains("state") || fieldRef.fieldNames().contains("country")
            case _ => false
          }
        case _ => false
      } && logicalPlan.asInstanceOf[CreateTableAsSelect].partitioning.length == 2,
      "Partitioning does not contain a FieldReferences: 'name'")
  }

  test("view using parquet partition by state & country") {
    val frame = sql(s"""
                       |view $viewName using parquet partitioned by (state, country) | source = $testTable | dedup name | fields name, state, country
                       | """.stripMargin)

    frame.collect()
    // verify new view was created correctly
    val results = sql(s"""
                         | source = $viewName
                         | """.stripMargin).collect()

    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jane", "Quebec", "Canada"),
      Row("John", "Ontario", "Canada"),
      Row("Jake", "California", "USA"),
      Row("Hello", "New York", "USA"))
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
      assert(
        expectedDescribeResults.contains(expectedRow),
        s"Expected row $expectedRow not found in results")
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
        Project(
          Seq(
            UnresolvedAttribute("name"),
            UnresolvedAttribute("state"),
            UnresolvedAttribute("country")),
          dedup),
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
    comparePlans(
      logicalPlan.asInstanceOf[CreateTableAsSelect].query,
      expectedPlan.asInstanceOf[CreateTableAsSelect].query,
      checkAnalysis = false)
    comparePlans(
      logicalPlan.asInstanceOf[CreateTableAsSelect].name,
      expectedPlan.asInstanceOf[CreateTableAsSelect].name,
      checkAnalysis = false)
    assert(
      logicalPlan.asInstanceOf[CreateTableAsSelect].tableSpec.toString == expectedPlan
        .asInstanceOf[CreateTableAsSelect]
        .tableSpec
        .toString)
    assert(
      logicalPlan.asInstanceOf[CreateTableAsSelect].partitioning.exists {
        case transform: Transform =>
          transform.arguments().exists {
            case fieldRef: NamedReference =>
              fieldRef.fieldNames().contains("state") || fieldRef.fieldNames().contains("country")
            case _ => false
          }
        case _ => false
      } && logicalPlan.asInstanceOf[CreateTableAsSelect].partitioning.length == 2,
      "Partitioning does not contain a FieldReferences: 'name'")
  }

  test("view using parquet with options & partition by state & country") {
    val frame = sql(s"""
                       | view $viewName using parquet OPTIONS('parquet.bloom.filter.enabled'='true', 'parquet.bloom.filter.enabled#age'='false')
                       | partitioned by (state, country) | source = $testTable | dedup name | fields name, state, country
                       | """.stripMargin)

    frame.collect()
    // verify new view was created correctly
    val results = sql(s"""
                         | source = $viewName
                         | """.stripMargin).collect()

    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jane", "Quebec", "Canada"),
      Row("John", "Ontario", "Canada"),
      Row("Jake", "California", "USA"),
      Row("Hello", "New York", "USA"))
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
      assert(
        expectedDescribeResults.contains(expectedRow),
        s"Expected row $expectedRow not found in results")
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
        Project(
          Seq(
            UnresolvedAttribute("name"),
            UnresolvedAttribute("state"),
            UnresolvedAttribute("country")),
          dedup),
        UnresolvedTableSpec(
          Map.empty,
          Option("PARQUET"),
          OptionList(
            Seq(
              ("parquet.bloom.filter.enabled", Literal("true")),
              ("parquet.bloom.filter.enabled#age", Literal("false")))),
          Option.empty,
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = false,
        isAnalyzed = false)
    // Compare the two plans
    comparePlans(
      logicalPlan.asInstanceOf[CreateTableAsSelect].query,
      expectedPlan.asInstanceOf[CreateTableAsSelect].query,
      checkAnalysis = false)
    comparePlans(
      logicalPlan.asInstanceOf[CreateTableAsSelect].name,
      expectedPlan.asInstanceOf[CreateTableAsSelect].name,
      checkAnalysis = false)
    assert(
      logicalPlan.asInstanceOf[CreateTableAsSelect].tableSpec.toString == expectedPlan
        .asInstanceOf[CreateTableAsSelect]
        .tableSpec
        .toString)
    assert(
      logicalPlan.asInstanceOf[CreateTableAsSelect].partitioning.exists {
        case transform: Transform =>
          transform.arguments().exists {
            case fieldRef: NamedReference =>
              fieldRef.fieldNames().contains("state") || fieldRef.fieldNames().contains("country")
            case _ => false
          }
        case _ => false
      } && logicalPlan.asInstanceOf[CreateTableAsSelect].partitioning.length == 2,
      "Partitioning does not contain a FieldReferences: 'name'")
  }

  test("view using parquet with options & location with partition by state & country") {
    val viewLocation = viewFolderLocation.toAbsolutePath.toString
    val frame = sql(s"""
                       | view $viewName using parquet OPTIONS('parquet.bloom.filter.enabled'='true', 'parquet.bloom.filter.enabled#age'='false')
                       | partitioned by (state, country) location '$viewLocation' | source = $testTable | dedup name | fields name, state, country
                       | """.stripMargin)

    frame.collect()
    // verify new view was created correctly
    val results = sql(s"""
                         | source = $viewName
                         | """.stripMargin).collect()

    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jane", "Quebec", "Canada"),
      Row("John", "Ontario", "Canada"),
      Row("Jake", "California", "USA"),
      Row("Hello", "New York", "USA"))
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
      assert(
        expectedDescribeResults.contains(expectedRow),
        s"Expected row $expectedRow not found in results")
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
        Project(
          Seq(
            UnresolvedAttribute("name"),
            UnresolvedAttribute("state"),
            UnresolvedAttribute("country")),
          dedup),
        UnresolvedTableSpec(
          Map.empty,
          Option("PARQUET"),
          OptionList(
            Seq(
              ("parquet.bloom.filter.enabled", Literal("true")),
              ("parquet.bloom.filter.enabled#age", Literal("false")))),
          Option(viewLocation),
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = false,
        isAnalyzed = false)
    comparePlans(
      logicalPlan.asInstanceOf[CreateTableAsSelect].query,
      expectedPlan.asInstanceOf[CreateTableAsSelect].query,
      checkAnalysis = false)
    comparePlans(
      logicalPlan.asInstanceOf[CreateTableAsSelect].name,
      expectedPlan.asInstanceOf[CreateTableAsSelect].name,
      checkAnalysis = false)
    assert(
      logicalPlan.asInstanceOf[CreateTableAsSelect].tableSpec.toString == expectedPlan
        .asInstanceOf[CreateTableAsSelect]
        .tableSpec
        .toString)
    assert(
      logicalPlan.asInstanceOf[CreateTableAsSelect].partitioning.exists {
        case transform: Transform =>
          transform.arguments().exists {
            case fieldRef: NamedReference =>
              fieldRef.fieldNames().contains("state") || fieldRef.fieldNames().contains("country")
            case _ => false
          }
        case _ => false
      } && logicalPlan.asInstanceOf[CreateTableAsSelect].partitioning.length == 2,
      "Partitioning does not contain a FieldReferences: 'name'")
  }

  test("test inner join with relation subquery") {
    val viewLocation = viewFolderLocation.toAbsolutePath.toString
    val frame = sql(s"""
                       | view $viewName using parquet OPTIONS('parquet.bloom.filter.enabled'='true')
                       | partitioned by (age_span) location '$viewLocation'
                       | | source = $testTable1
                       | | where country = 'USA' OR country = 'England'
                       | | inner join left=a, right=b
                       |     ON a.name = b.name
                       |     [
                       |       source = $testTable2
                       |       | where salary > 0
                       |       | fields name, country, salary
                       |       | sort salary
                       |       | head 3
                       |     ]
                       | | stats avg(salary) by span(age, 10) as age_span, b.country
                       | """.stripMargin)
    // verify new view was created correctly
    frame.collect()
    val results = sql(s"""
                         | source = $viewName
                         | """.stripMargin).collect()

    val expectedResults: Array[Row] = Array(Row(70000.0, "USA", 30), Row(100000.0, "England", 70))

    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))
    val filterExpr = Or(
      EqualTo(UnresolvedAttribute("country"), Literal("USA")),
      EqualTo(UnresolvedAttribute("country"), Literal("England")))
    val plan1 = SubqueryAlias("a", Filter(filterExpr, table1))
    val rightSubquery =
      GlobalLimit(
        Literal(3),
        LocalLimit(
          Literal(3),
          Sort(
            Seq(SortOrder(UnresolvedAttribute("salary"), Ascending)),
            global = true,
            Project(
              Seq(
                UnresolvedAttribute("name"),
                UnresolvedAttribute("country"),
                UnresolvedAttribute("salary")),
              Filter(GreaterThan(UnresolvedAttribute("salary"), Literal(0)), table2)))))
    val plan2 = SubqueryAlias("b", rightSubquery)

    val joinCondition = EqualTo(UnresolvedAttribute("a.name"), UnresolvedAttribute("b.name"))
    val joinPlan = Join(plan1, plan2, Inner, Some(joinCondition), JoinHint.NONE)

    val salaryField = UnresolvedAttribute("salary")
    val countryField = UnresolvedAttribute("b.country")
    val countryAlias = Alias(countryField, "b.country")()
    val star = Seq(UnresolvedStar(None))
    val aggregateExpressions =
      Alias(UnresolvedFunction(Seq("AVG"), Seq(salaryField), isDistinct = false), "avg(salary)")()
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val aggregatePlan =
      Aggregate(Seq(countryAlias, span), Seq(aggregateExpressions, countryAlias, span), joinPlan)

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq("simpleView")),
        Seq(),
        //        Seq(IdentityTransform.apply(FieldReference.apply("age_span"))),
        Project(star, aggregatePlan),
        UnresolvedTableSpec(
          Map.empty,
          Option("PARQUET"),
          OptionList(Seq(("parquet.bloom.filter.enabled", Literal("true")))),
          Option(viewLocation),
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = true,
        isAnalyzed = false)

    // Compare the two plans
    comparePlans(
      logicalPlan.asInstanceOf[CreateTableAsSelect].query,
      expectedPlan.asInstanceOf[CreateTableAsSelect].query,
      checkAnalysis = false)
    comparePlans(
      logicalPlan.asInstanceOf[CreateTableAsSelect].name,
      expectedPlan.asInstanceOf[CreateTableAsSelect].name,
      checkAnalysis = false)
    assert(
      logicalPlan.asInstanceOf[CreateTableAsSelect].tableSpec.toString == expectedPlan
        .asInstanceOf[CreateTableAsSelect]
        .tableSpec
        .toString)
    assert(
      logicalPlan.asInstanceOf[CreateTableAsSelect].partitioning.exists {
        case transform: Transform =>
          transform.arguments().exists {
            case fieldRef: NamedReference => fieldRef.fieldNames().contains("age_span")
            case _ => false
          }
        case _ => false
      } && logicalPlan.asInstanceOf[CreateTableAsSelect].partitioning.length == 1,
      "Partitioning does not contain a FieldReferences: 'name'")
  }
}
