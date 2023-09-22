/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.OpenSearchSuite

import org.apache.spark.FlintSuite
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.flint.config.FlintSparkConf.{HOST_ENDPOINT, HOST_PORT, REFRESH_POLICY}
import org.apache.spark.sql.streaming.StreamTest

/**
 * Flint Spark suite trait that initializes [[FlintSpark]] API instance.
 */
trait FlintSparkSuite
    extends QueryTest
    with FlintSuite
    with OpenSearchSuite
    with StreamTest {

  /** Flint Spark high level API being tested */
  lazy protected val flint: FlintSpark = new FlintSpark(spark)

  override def beforeAll(): Unit = {
    super.beforeAll()

    setFlintSparkConf(HOST_ENDPOINT, openSearchHost)
    setFlintSparkConf(HOST_PORT, openSearchPort)
    setFlintSparkConf(REFRESH_POLICY, "true")
  }

  protected def createPartitionedTable(testTable: String): Unit = {
    sql(
      s"""
         | CREATE TABLE $testTable
         | (
         |   time TIMESTAMP,
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

    sql(
      s"""
         | INSERT INTO $testTable
         | PARTITION (year=2023, month=4)
         | VALUES (TIMESTAMP '2023-09-21 16:30:00', 'Hello', 30, 'Seattle')
         | """.stripMargin)

    sql(
      s"""
         | INSERT INTO $testTable
         | PARTITION (year=2023, month=5)
         | VALUES (TIMESTAMP '2023-09-21 17:15:00', 'World', 25, 'Portland')
         | """.stripMargin)
  }
}
