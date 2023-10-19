/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql

import org.scalatest.matchers.should.Matchers

import org.apache.spark.FlintSuite

class FlintSparkSqlParserSuite extends FlintSuite with Matchers {

  test("create skipping index with filtering condition") {
    the[UnsupportedOperationException] thrownBy {
      sql("""
          | CREATE SKIPPING INDEX ON alb_logs
          | (client_ip VALUE_SET)
          | WHERE status != 200
          | WITH (auto_refresh = true)
          |""".stripMargin)
    } should have message "Filtering condition is not supported: WHERE status != 200"
  }

  test("create covering index with filtering condition") {
    the[UnsupportedOperationException] thrownBy {
      sql("""
          | CREATE INDEX test ON alb_logs
          | (elb, client_ip)
          | WHERE status != 404
          | WITH (auto_refresh = true)
          |""".stripMargin)
    } should have message "Filtering condition is not supported: WHERE status != 404"
  }
}
