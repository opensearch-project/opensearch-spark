/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.Option.empty

import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.getFlintIndexName
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.must.Matchers.have
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.Row

class FlintSparkIndexNameITSuite extends FlintSparkSuite {

  /** Test table that has table name and column name with uppercase letter */
  private val testTable = "spark_catalog.default.Test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    sql(s"""
         | CREATE TABLE $testTable
         | (
         |   Name STRING
         | )
         | USING CSV
         | OPTIONS (
         |  header 'false',
         |  delimiter '\t'
         | )
         |""".stripMargin)

    sql(s"""
         | INSERT INTO $testTable
         | VALUES ('Hello')
         | """.stripMargin)
  }

  test("skipping index with table and column name with uppercase letter") {
    sql(s"""
        | CREATE SKIPPING INDEX ON $testTable
        | ( Name VALUE_SET)
        |""".stripMargin)

    checkAnswer(
      sql(s"DESC SKIPPING INDEX ON $testTable"),
      Seq(Row("Name", "string", "VALUE_SET")))

    sql(s"REFRESH SKIPPING INDEX ON $testTable")
    val flintIndexName = getSkippingIndexName(testTable)
    val indexData = flint.queryIndex(flintIndexName).collect().toSet
    indexData should have size 1

    sql(s"DROP SKIPPING INDEX ON $testTable")
    flint.describeIndex(flintIndexName) shouldBe empty
  }

  test("covering index with index, table and column name with uppercase letter") {
    val testIndex = "Idx_Name"
    sql(s"""
         | CREATE INDEX $testIndex ON $testTable (Name)
         |""".stripMargin)

    checkAnswer(sql(s"SHOW INDEX ON $testTable"), Seq(Row(testIndex)))
    checkAnswer(
      sql(s"DESC INDEX $testIndex ON $testTable"),
      Seq(Row("Name", "string", "indexed")))

    sql(s"REFRESH INDEX $testIndex ON $testTable")
    val flintIndexName = getFlintIndexName(testIndex, testTable)
    val indexData = flint.queryIndex(flintIndexName).collect().toSet
    indexData should have size 1

    sql(s"DROP INDEX $testIndex ON $testTable")
    flint.describeIndex(flintIndexName) shouldBe empty
  }
}
