/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite

class FlintSparkIndexBuilderSuite extends FlintSuite {

  override def beforeAll(): Unit = {
    super.beforeAll()

    sql("""
        | CREATE TABLE spark_catalog.default.test
        | ( name STRING, age INT )
        | USING JSON
      """.stripMargin)
  }

  protected override def afterAll(): Unit = {
    sql("DROP TABLE spark_catalog.default.test")

    super.afterAll()
  }

  test("should qualify table name in default database") {
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

  test("should qualify table name and get columns in other database") {
    sql("CREATE DATABASE mydb")
    sql("CREATE TABLE mydb.test2 (address STRING) USING JSON")
    sql("USE mydb")

    try {
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
        .onTable("spark_catalog.default.test")
        .expectTableName("spark_catalog.default.test")
        .expectAllColumns("name", "age")
    } finally {
      sql("DROP DATABASE mydb CASCADE")
      sql("USE default")
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

    override protected def buildIndex(): FlintSparkIndex = {
      null
    }
  }
}
