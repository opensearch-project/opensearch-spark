/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

class FlintSparkCoveringIndexITSuite extends FlintSparkIndexSuite {

  /** Test table and index name */
  private val testTable = "default.test"
  private val testIndex = "test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    sql(s"""
           | CREATE TABLE $testTable
           | (
           |   name STRING,
           |   age INT,
           |   address STRING
           | )
           | USING CSV
           | OPTIONS (
           |  header 'false',
           |  delimiter '\t'
           | )
           | PARTITIONED BY (
           |    year INT,
           |    month INT
           | )
           |""".stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=4)
           | VALUES ('Hello', 30, 'Seattle')
           | """.stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=5)
           | VALUES ('World', 25, 'Portland')
           | """.stripMargin)
  }

  override def afterEach(): Unit = {
    super.afterEach()

    // Delete all test indices
    flint.deleteIndex(testIndex)
  }

  test("create covering index with metadata successfully") {

  }
}
