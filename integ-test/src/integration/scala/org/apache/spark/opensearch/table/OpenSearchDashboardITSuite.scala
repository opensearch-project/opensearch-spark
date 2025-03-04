/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.table

import org.opensearch.flint.spark.ppl.FlintPPLSuite

import org.apache.spark.sql.Row

class OpenSearchDashboardITSuite extends OpenSearchCatalogSuite with FlintPPLSuite {
  test("test dashboards queries") {
    Seq(dashboards_sample_data_flights()).foreach { config =>
      withIndexName(config.index) {
        openSearchDashboardsIndex(config.useCaseName, config.index)
        config.tests.foreach { sqlTest =>
          sqlTest.queries.foreach { query =>
            val df = spark.sql(query)
            withClue(s"Failed query: ${query}\n") {
              checkAnswer(df, sqlTest.expected)
            }
          }
        }
      }
    }
  }

  case class QueryTest(queries: Seq[String], expected: Seq[Row])
  case class TestConfig(useCaseName: String, index: String, tests: Seq[QueryTest])

  def dashboards_sample_data_flights(): TestConfig = {
    val tbl = "flights"
    TestConfig(
      "dashboards_sample_data_flights",
      tbl,
      Seq(
        // Airline Carrier
        QueryTest(
          Seq(
            s"""SELECT carrier, COUNT(*) as count
              FROM dev.default.$tbl
              GROUP BY Carrier ORDER BY count DESC""",
            s"""source=dev.default.$tbl
               || stats count() as count by Carrier
               || sort - count
               || fields Carrier, count""".stripMargin),
          Seq(Row("OpenSearch Dashboards Airlines", 3), Row("Logstash Airways", 2))),
        // Average ticket price
        QueryTest(
          Seq(
            s"""SELECT date_format(window.start, 'yyyy-MM-dd HH:mm:ss'), MAX(AvgTicketPrice) AS
             |avg_ticket_price
             | FROM dev.default.$tbl
             | GROUP BY window(timestamp,'1 day') ORDER BY window.start
             |""".stripMargin,
            s"""source=dev.default.$tbl
               || stats max(AvgTicketPrice) as avg by span(timestamp, 1d) as window
               || sort window.start
               || eval start = date_format(window.start, 'yyyy-MM-dd HH:mm:ss')
               || fields start, avg""".stripMargin),
          Seq(Row("2025-01-27 00:00:00", 882.98267f))),
        // Total Flight Delays
        QueryTest(
          Seq(
            s"SELECT COUNT(*) as count FROM dev.default.$tbl WHERE FlightDelay = true",
            s"""source=dev.default.$tbl | where FlightDelay=True | stats count()"""),
          Seq(Row(1))),
        // Flight Delays
        QueryTest(
          Seq(
            s"""SELECT FlightDelay, COUNT(*) as count FROM dev.default.$tbl GROUP BY FlightDelay""",
            s"""source=dev.default.$tbl
               || stats count() as cnt by FlightDelay
               || fields FlightDelay, cnt""".stripMargin),
          Seq(Row(false, 4), Row(true, 1))),
        // Delay Buckets
        QueryTest(
          Seq(
            s"SELECT FLOOR(FlightDelayMin / 30) * 30 AS bucket, COUNT(*) AS doc_count FROM dev" +
              s".default.$tbl GROUP BY bucket ORDER BY bucket",
            s"""source=dev.default.$tbl
               || eval bucket=FLOOR(FlightDelayMin / 30) * 30
               || stats count() as cnt by bucket
               || sort bucket
               || fields bucket, cnt""".stripMargin),
          Seq(Row(0, 4), Row(180, 1)))))
  }
}
