/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.scalatest.matchers.must.Matchers.have
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, the}

import org.apache.spark.sql.FlintSparkHiveSuite

class FlintSparkHiveValidationITSuite extends FlintSparkHiveSuite {

  private val hiveTableName = "spark_catalog.default.hive_table"

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE TABLE $hiveTableName (name STRING)")
  }

  override def afterAll(): Unit = {
    sql(s"DROP TABLE $hiveTableName")
    super.afterAll()
  }

  test("should fail if create auto refresh skipping index on Hive table") {
    the[IllegalArgumentException] thrownBy {
      sql(s"""
           | CREATE SKIPPING INDEX ON $hiveTableName
           | ( name VALUE_SET )
           | WITH (auto_refresh = true)
           | """.stripMargin)
    } should have message "requirement failed: Index auto refresh doesn't support Hive table"
  }

  test("should fail if create incremental refresh skipping index on Hive table") {
    the[IllegalArgumentException] thrownBy {
      sql(s"""
             | CREATE SKIPPING INDEX ON $hiveTableName
             | ( name VALUE_SET )
             | WITH (incremental_refresh = true)
             | """.stripMargin)
    } should have message "requirement failed: Index incremental refresh doesn't support Hive table"
  }
}
