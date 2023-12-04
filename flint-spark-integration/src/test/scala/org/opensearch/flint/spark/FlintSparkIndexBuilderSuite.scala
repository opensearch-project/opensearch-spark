/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite

class FlintSparkIndexBuilderSuite extends FlintSuite {

  test("should return all columns") {
    val testTable = "spark_catalog.default.index_builder_test"
    withTable(testTable) {
      sql(s"""
             | CREATE TABLE $testTable
             | ( name STRING, age INT )
             | USING JSON
      """.stripMargin)

      builder()
        .onTable(testTable)
        .expectAllColumns("name", "age")
    }
  }

  test("should return all partitioned columns") {
    val testTable = "spark_catalog.default.index_builder_test"
    withTable(testTable) {
      sql(s"""
             | CREATE TABLE $testTable
             | ( name STRING, age INT )
             | USING JSON
             | PARTITIONED BY ( year INT, month INT )
      """.stripMargin)

      builder()
        .onTable(testTable)
        .expectAllColumns("year", "month", "name", "age")
        .expectPartitionColumns("year", "month")
    }
  }

  test("should qualify table name in default database") {
    val testTable = "spark_catalog.default.test"
    withTable(testTable) {
      sql(s"""
            | CREATE TABLE $testTable
            | ( name STRING, age INT )
            | USING JSON
      """.stripMargin)

      builder()
        .onTable("test")
        .expectTableName("spark_catalog.default.test")
        .expectAllColumns("name", "age")

      builder()
        .onTable("default.test")
        .expectTableName("spark_catalog.default.test")
        .expectAllColumns("name", "age")

      builder()
        .onTable("spark_catalog.default.test")
        .expectTableName("spark_catalog.default.test")
        .expectAllColumns("name", "age")
    }
  }

  test("should qualify table name and get columns in other database") {
    withDatabase("mydb") {
      val testTable = "spark_catalog.default.index_builder_test"
      withTable(testTable) {
        // Create a table in default database
        sql(s"""
             | CREATE TABLE $testTable
             | ( name STRING, age INT )
             | USING JSON
      """.stripMargin)

        // Create another database and table and switch database
        sql("CREATE DATABASE mydb")
        sql("CREATE TABLE mydb.test2 (address STRING) USING JSON")
        sql("USE mydb")

        builder()
          .onTable("test2")
          .expectTableName("spark_catalog.mydb.test2")
          .expectAllColumns("address")

        builder()
          .onTable("mydb.test2")
          .expectTableName("spark_catalog.mydb.test2")
          .expectAllColumns("address")

        builder()
          .onTable("spark_catalog.mydb.test2")
          .expectTableName("spark_catalog.mydb.test2")
          .expectAllColumns("address")

        // Can parse any specified table name
        builder()
          .onTable(testTable)
          .expectTableName(testTable)
          .expectAllColumns("name", "age")

        builder()
          .onTable("default.index_builder_test")
          .expectTableName(testTable)
          .expectAllColumns("name", "age")
      }
    }
  }

  private def builder(): FakeFlintSparkIndexBuilder = {
    new FakeFlintSparkIndexBuilder
  }

  /**
   * Fake builder that have access to internal method for assertion
   */
  class FakeFlintSparkIndexBuilder extends FlintSparkIndexBuilder(new FlintSpark(spark)) {

    def onTable(tableName: String): FakeFlintSparkIndexBuilder = {
      this.tableName = tableName
      this
    }

    def expectTableName(expected: String): FakeFlintSparkIndexBuilder = {
      tableName shouldBe expected
      this
    }

    def expectAllColumns(expected: String*): FakeFlintSparkIndexBuilder = {
      allColumns.keys should contain theSameElementsAs expected
      this
    }

    def expectPartitionColumns(expected: String*): FakeFlintSparkIndexBuilder = {
      allColumns.values
        .filter(_.isPartition)
        .map(_.name) should contain theSameElementsAs expected
      this
    }

    override protected def buildIndex(): FlintSparkIndex = {
      null
    }
  }
}
