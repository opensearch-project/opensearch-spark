/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.{Locale, UUID}

import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.RefreshMode.{AUTO, INCREMENTAL, RefreshMode}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.scalatest.matchers.must.Matchers.have
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, the}

import org.apache.spark.sql.SparkHiveSupportSuite
import org.apache.spark.sql.flint.config.FlintSparkConf.CHECKPOINT_MANDATORY

class FlintSparkIndexValidationITSuite extends FlintSparkSuite with SparkHiveSupportSuite {

  // Test Hive table name
  private val testTable = "spark_catalog.default.index_validation_test"

  // Test create Flint index name and DDL statement
  private val skippingIndexName = FlintSparkSkippingIndex.getSkippingIndexName(testTable)
  private val createSkippingIndexStatement =
    s"CREATE SKIPPING INDEX ON $testTable (name VALUE_SET)"

  private val coveringIndexName =
    FlintSparkCoveringIndex.getFlintIndexName("ci_test", testTable)
  private val createCoveringIndexStatement =
    s"CREATE INDEX ci_test ON $testTable (name)"

  private val materializedViewName =
    FlintSparkMaterializedView.getFlintIndexName("spark_catalog.default.mv_test")
  private val createMaterializedViewStatement =
    s"CREATE MATERIALIZED VIEW spark_catalog.default.mv_test AS SELECT * FROM $testTable"

  Seq(createSkippingIndexStatement, createCoveringIndexStatement, createMaterializedViewStatement)
    .foreach { statement =>
      test(
        s"should fail to create auto refresh Flint index if incremental refresh enabled: $statement") {
        withTable(testTable) {
          sql(s"CREATE TABLE $testTable (name STRING) USING JSON")

          the[IllegalArgumentException] thrownBy {
            sql(s"""
                 | $statement
                 | WITH (
                 |   auto_refresh = true,
                 |   incremental_refresh = true
                 | )
                 |""".stripMargin)
          } should have message
            "requirement failed: Incremental refresh cannot be enabled if auto refresh is enabled"
        }
      }
    }

  Seq(createSkippingIndexStatement, createCoveringIndexStatement, createMaterializedViewStatement)
    .foreach { statement =>
      test(
        s"should fail to create auto refresh Flint index if checkpoint location mandatory: $statement") {
        withTable(testTable) {
          sql(s"CREATE TABLE $testTable (name STRING) USING JSON")

          the[IllegalArgumentException] thrownBy {
            try {
              setFlintSparkConf(CHECKPOINT_MANDATORY, "true")
              sql(s"""
                   | $statement
                   | WITH (
                   |   auto_refresh = true
                   | )
                   |""".stripMargin)
            } finally {
              setFlintSparkConf(CHECKPOINT_MANDATORY, "false")
            }
          } should have message
            s"requirement failed: Checkpoint location is required if ${CHECKPOINT_MANDATORY.key} option enabled"
        }
      }
    }

  Seq(createSkippingIndexStatement, createCoveringIndexStatement, createMaterializedViewStatement)
    .foreach { statement =>
      test(
        s"should fail to create incremental refresh Flint index without checkpoint location: $statement") {
        withTable(testTable) {
          sql(s"CREATE TABLE $testTable (name STRING) USING JSON")

          the[IllegalArgumentException] thrownBy {
            sql(s"""
                 | $statement
                 | WITH (
                 |   incremental_refresh = true
                 | )
                 |""".stripMargin)
          } should have message
            "requirement failed: Checkpoint location is required by incremental refresh"
        }
      }
    }

  Seq(
    (AUTO, createSkippingIndexStatement),
    (AUTO, createCoveringIndexStatement),
    (AUTO, createMaterializedViewStatement),
    (INCREMENTAL, createSkippingIndexStatement),
    (INCREMENTAL, createCoveringIndexStatement),
    (INCREMENTAL, createMaterializedViewStatement))
    .foreach { case (refreshMode, statement) =>
      test(
        s"should fail to create $refreshMode refresh Flint index if checkpoint location is inaccessible: $statement") {
        withTable(testTable) {
          sql(s"CREATE TABLE $testTable (name STRING) USING JSON")

          // Generate UUID as folder name to ensure the path not exist
          val checkpointDir = s"/test/${UUID.randomUUID()}"
          the[IllegalArgumentException] thrownBy {
            sql(s"""
                 | $statement
                 | WITH (
                 |   ${optionName(refreshMode)} = true,
                 |   checkpoint_location = "$checkpointDir"
                 | )
                 |""".stripMargin)
          } should have message
            s"requirement failed: Checkpoint location $checkpointDir doesn't exist or no permission to access"
        }
      }
    }

  Seq(
    (AUTO, createSkippingIndexStatement),
    (AUTO, createCoveringIndexStatement),
    (AUTO, createMaterializedViewStatement),
    (INCREMENTAL, createSkippingIndexStatement),
    (INCREMENTAL, createCoveringIndexStatement),
    (INCREMENTAL, createMaterializedViewStatement))
    .foreach { case (refreshMode, statement) =>
      test(s"should fail to create $refreshMode refresh Flint index on Hive table: $statement") {
        withTempDir { checkpointDir =>
          withTable(testTable) {
            sql(s"CREATE TABLE $testTable (name STRING)")

            the[IllegalArgumentException] thrownBy {
              sql(s"""
                   | $statement
                   | WITH (
                   |   ${optionName(refreshMode)} = true,
                   |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
                   | )
                   |""".stripMargin)
            } should have message
              s"requirement failed: Index ${lowercase(refreshMode)} refresh doesn't support Hive table"
          }
        }
      }
    }

  Seq(
    (skippingIndexName, createSkippingIndexStatement),
    (coveringIndexName, createCoveringIndexStatement),
    (materializedViewName, createMaterializedViewStatement)).foreach {
    case (flintIndexName, statement) =>
      test(s"should succeed to create full refresh Flint index on Hive table: $flintIndexName") {
        withTable(testTable) {
          sql(s"CREATE TABLE $testTable (name STRING)")
          sql(s"INSERT INTO $testTable VALUES ('test')")

          sql(statement)
          flint.refreshIndex(flintIndexName)
          flint.queryIndex(flintIndexName).count() shouldBe 1
        }
      }
  }

  private def lowercase(mode: RefreshMode): String = mode.toString.toLowerCase(Locale.ROOT)

  private def optionName(mode: RefreshMode): String = mode match {
    case AUTO => "auto_refresh"
    case INCREMENTAL => "incremental_refresh"
  }
}
