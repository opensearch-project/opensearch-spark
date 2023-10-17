/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql

import org.scalatest.matchers.should.Matchers.the

import org.apache.spark.FlintSuite

class FlintSparkSqlSuite extends FlintSuite {

  test("materialize view statement is not supported yet") {
    the[UnsupportedOperationException] thrownBy
      spark.sql("""
        | CREATE MATERIALIZED VIEW spark_catalog.default.alb_logs_metrics
        | AS
        | SELECT elb, COUNT(*)
        | FROM alb_logs
        | GROUP BY TUMBLE(time, '1 Minute')
        | WITH (
        |   auto_refresh = true
        | )
        |""".stripMargin)

    the[UnsupportedOperationException] thrownBy
      spark.sql("DROP MATERIALIZED VIEW spark_catalog.default.alb_log_metrics")
  }
}
