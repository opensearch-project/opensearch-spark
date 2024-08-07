/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSessionBase

class FlintDelegatingSessionCatalogTest extends QueryTest with SharedSparkSessionBase {
  private val testTable = "mycatalog.default.flint_sql_test"
  private val testTableWithoutCatalog = "default.flint_sql_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.conf.set(
      "spark.sql.catalog.mycatalog",
      "org.opensearch.sql.FlintDelegatingSessionCatalog")

    sql(s"""
                 | CREATE TABLE $testTable
                 | (
                 |   name STRING,
                 |   age INT
                 | )
                 | USING CSV
                 | OPTIONS (
                 |  header 'false',
                 |  delimiter '\t'
                 | )
                 |""".stripMargin)

    sql(s"""
                 | INSERT INTO $testTable
                 | VALUES ('Hello', 30)
                 | """.stripMargin)
  }

  test("query with customized catalog name") {
    var result = sql(s"SELECT name, age FROM $testTable")
    checkAnswer(result, Seq(Row("Hello", 30)))
  }

  test("query without catalog name") {
    sql("use mycatalog")
    // Since Spark 3.4.0. https://issues.apache.org/jira/browse/SPARK-40055, listCatalogs should
    // also return spark_catalog even spark_catalog implementation is defaultSessionCatalog
    assert(
      sql("SHOW CATALOGS").collect.toSet === Array(Row("mycatalog"), Row("spark_catalog")).toSet)

    checkAnswer(sql(s"SELECT name, age FROM $testTableWithoutCatalog"), Seq(Row("Hello", 30)))
  }
}
