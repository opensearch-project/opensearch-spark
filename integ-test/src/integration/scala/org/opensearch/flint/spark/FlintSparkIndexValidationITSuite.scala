/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.{Locale, UUID}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FSDataInputStream, Path, PathFilter}
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.RefreshMode.{AUTO, INCREMENTAL, RefreshMode}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.scalatest.matchers.must.Matchers.have
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, the}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.sql.SparkHiveSupportSuite
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.flint.config.FlintSparkConf.CHECKPOINT_MANDATORY
import org.apache.spark.sql.internal.SQLConf

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
      ignore(
        s"should fail to create auto refresh Flint index if scheduler_mode is external and no checkpoint location: $statement") {
        withTable(testTable) {
          sql(s"CREATE TABLE $testTable (name STRING) USING JSON")

          the[IllegalArgumentException] thrownBy {
            sql(s"""
                 | $statement
                 | WITH (
                 |   auto_refresh = true,
                 |   scheduler_mode = 'external'
                 | )
                 |""".stripMargin)
          } should have message
            "requirement failed: Checkpoint location is required"
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
            "requirement failed: Checkpoint location is required"
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
        s"should fail to create $refreshMode refresh Flint index if checkpoint location is not writable: $statement") {
        withTable(testTable) {
          sql(s"CREATE TABLE $testTable (name STRING) USING JSON")

          withTempDir { checkpointDir =>
            // Set checkpoint dir readonly to simulate the exception
            checkpointDir.setWritable(false)

            the[IllegalArgumentException] thrownBy {
              sql(s"""
                   | $statement
                   | WITH (
                   |   ${optionName(refreshMode)} = true,
                   |   checkpoint_location = "$checkpointDir"
                   | )
                   |""".stripMargin)
            } should have message
              s"requirement failed: No sufficient permission to access the checkpoint location $checkpointDir"
          }
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

          // Use unknown scheme URL to simulate location without access permission
          val checkpointDir = "unknown://invalid_permission_path"
          the[IllegalArgumentException] thrownBy {
            sql(s"""
                 | $statement
                 | WITH (
                 |   ${optionName(refreshMode)} = true,
                 |   checkpoint_location = "$checkpointDir"
                 | )
                 |""".stripMargin)
          } should have message
            s"requirement failed: No sufficient permission to access the checkpoint location $checkpointDir"
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

          deleteTestIndex(flintIndexName)
        }
      }
  }

  Seq(
    (skippingIndexName, AUTO, createSkippingIndexStatement),
    (coveringIndexName, AUTO, createCoveringIndexStatement),
    (materializedViewName, AUTO, createMaterializedViewStatement),
    (skippingIndexName, INCREMENTAL, createSkippingIndexStatement),
    (coveringIndexName, INCREMENTAL, createCoveringIndexStatement),
    (materializedViewName, INCREMENTAL, createMaterializedViewStatement))
    .foreach { case (flintIndexName, refreshMode, statement) =>
      test(
        s"should succeed to create $refreshMode refresh Flint index even if checkpoint sub-folder doesn't exist: $statement") {
        withTable(testTable) {
          sql(s"CREATE TABLE $testTable (name STRING) USING JSON")
          sql(s"INSERT INTO $testTable VALUES ('test')")

          withTempDir { checkpointDir =>
            // Specify nonexistent sub-folder and expect pre-validation to pass
            val nonExistCheckpointDir = s"$checkpointDir/${UUID.randomUUID().toString}"
            sql(s"""
                 | $statement
                 | WITH (
                 |   ${optionName(refreshMode)} = true,
                 |   checkpoint_location = '$nonExistCheckpointDir'
                 | )
                 |""".stripMargin)

            deleteTestIndex(flintIndexName)
          }
        }
      }
    }

  test(
    "should bypass write permission check for checkpoint location if checkpoint manager class doesn't support create temp file") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (name STRING) USING JSON")
      sql(s"INSERT INTO $testTable VALUES ('test')")

      withTempDir { checkpointDir =>
        // Set readonly to verify write permission check bypass
        checkpointDir.setWritable(false)

        // Configure fake checkpoint file manager
        val confKey = SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key
        withSQLConf(confKey -> classOf[FakeCheckpointFileManager].getName) {
          sql(s"""
               | $createSkippingIndexStatement
               | WITH (
               |   incremental_refresh = true,
               |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
               | )
               |""".stripMargin)
        }
      }
    }
  }

  private def lowercase(mode: RefreshMode): String = mode.toString.toLowerCase(Locale.ROOT)

  private def optionName(mode: RefreshMode): String = mode match {
    case AUTO => "auto_refresh"
    case INCREMENTAL => "incremental_refresh"
  }
}

/**
 * Fake checkpoint file manager.
 */
class FakeCheckpointFileManager(path: Path, conf: Configuration) extends CheckpointFileManager {

  override def createAtomic(
      path: Path,
      overwriteIfPossible: Boolean): CheckpointFileManager.CancellableFSDataOutputStream =
    throw new UnsupportedOperationException

  override def open(path: Path): FSDataInputStream = mock[FSDataInputStream]

  override def list(path: Path, filter: PathFilter): Array[FileStatus] = Array()

  override def mkdirs(path: Path): Unit = throw new UnsupportedOperationException

  override def exists(path: Path): Boolean = true

  override def delete(path: Path): Unit = throw new UnsupportedOperationException

  override def isLocal: Boolean = throw new UnsupportedOperationException

  override def createCheckpointDirectory(): Path = path
}
