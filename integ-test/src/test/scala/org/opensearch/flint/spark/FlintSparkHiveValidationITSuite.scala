/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.scalatest.matchers.must.Matchers.have
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, the}

import org.apache.spark.sql.FlintSparkHiveSuite

class FlintSparkHiveValidationITSuite extends FlintSparkHiveSuite {

  // Test Hive table name
  private val hiveTableName = "spark_catalog.default.hive_table"

  // Test create Flint index name and DDL statement
  private val skippingIndexName = FlintSparkSkippingIndex.getSkippingIndexName(hiveTableName)
  private val createSkippingIndexStatement =
    s"CREATE SKIPPING INDEX ON $hiveTableName (name VALUE_SET)"

  private val coveringIndexName =
    FlintSparkCoveringIndex.getFlintIndexName("ci_test", hiveTableName)
  private val createCoveringIndexStatement =
    s"CREATE INDEX ci_test ON $hiveTableName (name)"

  private val materializedViewName =
    FlintSparkMaterializedView.getFlintIndexName("spark_catalog.default.mv_test")
  private val createMaterializedViewStatement =
    s"CREATE MATERIALIZED VIEW spark_catalog.default.mv_test AS SELECT * FROM $hiveTableName"

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE TABLE $hiveTableName (name STRING)")
    sql(s"INSERT INTO $hiveTableName VALUES ('test')")
  }

  override def afterAll(): Unit = {
    sql(s"DROP TABLE $hiveTableName")
    super.afterAll()
  }

  Seq(createSkippingIndexStatement, createCoveringIndexStatement, createMaterializedViewStatement)
    .foreach { statement =>
      test(s"should fail to create auto refresh Flint index on Hive table: $statement") {
        the[IllegalArgumentException] thrownBy {
          withTempDir { checkpointDir =>
            sql(s"""
                 | $statement
                 | WITH (
                 |   auto_refresh = true,
                 |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
                 | )
                 |""".stripMargin)
          }
        } should have message "requirement failed: Index auto refresh doesn't support Hive table"
      }
    }

  Seq(createSkippingIndexStatement, createCoveringIndexStatement, createMaterializedViewStatement)
    .foreach { statement =>
      test(s"should fail to create incremental refresh Flint index on Hive table: $statement") {
        the[IllegalArgumentException] thrownBy {
          withTempDir { checkpointDir =>
            sql(s"""
                   | $statement
                   | WITH (
                   |   incremental_refresh = true,
                   |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
                   | )
                   |""".stripMargin)
          }
        } should have message "requirement failed: Index incremental refresh doesn't support Hive table"
      }
    }

  Seq(
    (skippingIndexName, createSkippingIndexStatement),
    (coveringIndexName, createCoveringIndexStatement),
    (materializedViewName, createMaterializedViewStatement)).foreach {
    case (flintIndexName, statement) =>
      test(s"should succeed to create full refresh Flint index on Hive table: $flintIndexName") {
        sql(statement)
        flint.refreshIndex(flintIndexName)
        flint.queryIndex(flintIndexName).count() shouldBe 1
      }
  }
}
