/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.table

import org.opensearch.flint.spark.ppl.FlintPPLSuite
import org.opensearch.flint.spark.udt.{GeoPoint, IPAddress}

import org.apache.spark.sql.Row

class OpenSearchDashboardITSuite extends OpenSearchCatalogSuite with FlintPPLSuite {
  test("test dashboards queries") {
    Seq(dashboards_sample_data_flights(), dashboards_sample_data_logs()).foreach { config =>
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

  def dashboards_sample_data_logs(): TestConfig = {
    val tbl = "logs"
    TestConfig(
      "dashboards_sample_data_logs",
      tbl,
      Seq(
        // Count by host
        QueryTest(
          Seq(
            s"""SELECT host, COUNT(*) AS count
               | FROM dev.default.$tbl
               | GROUP BY host
               | ORDER BY count DESC""".stripMargin,
            s"""source=dev.default.$tbl
               || stats count() as count by host
               || sort - count
               || fields host, count""".stripMargin),
          Seq(
            Row("www.opensearch.org", 2),
            Row("artifacts.opensearch.org", 2),
            Row("cdn.opensearch-opensearch-opensearch.org", 1))),
        // Average bytes per response code
        QueryTest(
          Seq(
            s"""SELECT response, AVG(bytes) as avg_bytes
               | FROM dev.default.$tbl
               | GROUP BY response
               | ORDER BY response""".stripMargin,
            s"""source=dev.default.$tbl
               || stats avg(bytes) as avg_bytes by response
               || sort response
               || fields response, avg_bytes""".stripMargin),
          Seq(Row("200", 7418.5), Row("503", 0.0))),
        // Select ip and geo_point fields
        QueryTest(
          Seq(
            s"""SELECT ip, geo.coordinates
               | FROM dev.default.$tbl
               | WHERE response = '503'""".stripMargin,
            s"""source=dev.default.$tbl
               || where response='503'
               || fields ip, geo.coordinates""".stripMargin),
          Seq(Row(IPAddress("120.49.143.213"), GeoPoint(36.96015, -78.18499861))))))
  }
}
