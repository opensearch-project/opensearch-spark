/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite

class FlintSparkIndexBuilderSuite extends FlintSuite {

  override def beforeAll(): Unit = {
    super.beforeAll()

    sql("""
        | CREATE TABLE spark_catalog.default.test
        | (
        |   name STRING,
        |   age INT,
        |   address STRUCT<first: STRING, second: STRUCT<city: STRING, street: STRING>>
        | )
        | USING JSON
      """.stripMargin)
  }

  protected override def afterAll(): Unit = {
    sql("DROP TABLE spark_catalog.default.test")

    super.afterAll()
  }

  test("find column type") {
    builder()
      .onTable("test")
      .expectTableName("spark_catalog.default.test")
      .expectColumn("name", "string")
      .expectColumn("age", "int")
      .expectColumn("address", "struct<first:string,second:struct<city:string,street:string>>")
      .expectColumn("address.first", "string")
      .expectColumn("address.second", "struct<city:string,street:string>")
      .expectColumn("address.second.city", "string")
      .expectColumn("address.second.street", "string")
  }

  test("should qualify table name in default database") {
    builder()
      .onTable("test")
      .expectTableName("spark_catalog.default.test")

    builder()
      .onTable("default.test")
      .expectTableName("spark_catalog.default.test")

    builder()
      .onTable("spark_catalog.default.test")
      .expectTableName("spark_catalog.default.test")
  }

  test("should qualify table name and get columns in other database") {
    sql("CREATE DATABASE mydb")
    sql("CREATE TABLE mydb.test2 (address STRING) USING JSON")
    sql("USE mydb")

    try {
      builder()
        .onTable("test2")
        .expectTableName("spark_catalog.mydb.test2")

      builder()
        .onTable("mydb.test2")
        .expectTableName("spark_catalog.mydb.test2")

      builder()
        .onTable("spark_catalog.mydb.test2")
        .expectTableName("spark_catalog.mydb.test2")

      // Can parse any specified table name
      builder()
        .onTable("spark_catalog.default.test")
        .expectTableName("spark_catalog.default.test")
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

    def expectColumn(expectName: String, expectType: String): FakeFlintSparkIndexBuilder = {
      val column = findColumn(expectName)
      column.name shouldBe expectName
      column.dataType shouldBe expectType
      this
    }

    override protected def buildIndex(): FlintSparkIndex = {
      null
    }
  }
}
