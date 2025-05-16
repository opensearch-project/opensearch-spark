/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.opensearch.search.builder.SearchSourceBuilder

import org.apache.spark.sql.Row

class FlintSparkMaterializedViewIntegrationVPCITSuite
    extends FlintSparkMaterializedViewIntegrationsITSuite {

  val mvQueryVPC: String = s"""
                              |SELECT
                              |  TUMBLE(`@timestamp`, '5 Minute').start AS `start_time`,
                              |  action AS `aws.vpc.action`,
                              |  srcAddr AS `aws.vpc.srcaddr`,
                              |  dstAddr AS `aws.vpc.dstaddr`,
                              |  protocol AS `aws.vpc.protocol`,
                              |  COUNT(*) AS `aws.vpc.total_count`,
                              |  SUM(bytes) AS `aws.vpc.total_bytes`,
                              |  SUM(packets) AS `aws.vpc.total_packets`
                              |FROM (
                              |  SELECT
                              |    action,
                              |    srcAddr,
                              |    dstAddr,
                              |    bytes,
                              |    packets,
                              |    protocol,
                              |    CAST(FROM_UNIXTIME(start) AS TIMESTAMP) AS `@timestamp`
                              |  FROM
                              |    $catalogName.default.vpc_low_test
                              |)
                              |GROUP BY
                              |  TUMBLE(`@timestamp`, '5 Minute'),
                              |  action,
                              |  srcAddr,
                              |  dstAddr,
                              |  protocol
                              |""".stripMargin

  val dslQueryBuilderVPCTimeSeriesChart: SearchSourceBuilder = {
    val builder = new SearchSourceBuilder()
      .query(QueryBuilders.wrapperQuery(dslQueryString))
    val dateHistogramAgg = AggregationBuilders
      .dateHistogram("2")
      .field("start_time")
      .minDocCount(1)
      .fixedInterval(DateHistogramInterval.minutes(5))

    val sumAgg = AggregationBuilders
      .sum("1")
      .field("aws.vpc.total_bytes")

    dateHistogramAgg.subAggregation(sumAgg)
    builder.aggregation(dateHistogramAgg)

    builder
  }

  val dslQueryBuilderVPCPieChart: SearchSourceBuilder = {
    val builder = new SearchSourceBuilder()
      .query(QueryBuilders.wrapperQuery(dslQueryString))
    val termsAgg = AggregationBuilders.terms("2").field("aws.vpc.srcaddr")

    val sumAgg = AggregationBuilders
      .sum("1")
      .field("aws.vpc.total_bytes")

    termsAgg.subAggregation(sumAgg)
    builder.aggregation(termsAgg)

    builder
  }

  val expectedBucketsVPCTimeSeriesChart: Seq[Map[String, Any]] = Seq(
    Map("key_as_string" -> "2023-11-01T05:00:00.000Z", "doc_count" -> 1),
    Map("key_as_string" -> "2023-11-01T05:10:00.000Z", "doc_count" -> 2))

  val expectedBucketsVPCPieChart: Seq[Map[String, Any]] = Seq(
    Map("key" -> "10.0.0.1", "doc_count" -> 1),
    Map("key" -> "10.0.0.3", "doc_count" -> 1),
    Map("key" -> "10.0.0.5", "doc_count" -> 1))

  test(
    "create aggregated materialized view for VPC flow integration unfiltered time series chart") {
    withIntegration("vpc_flow") { integration =>
      integration
        .createSourceTable(s"$catalogName.default.vpc_low_test")
        .createMaterializedView(mvQueryVPC, sourceDisabled = true)
        .assertDslQueryTimeSeriesChart(
          dslQueryBuilderVPCTimeSeriesChart,
          expectedBucketsVPCTimeSeriesChart)
    }
  }

  test("create aggregated materialized view for VPC flow integration unfiltered pie chart") {
    withIntegration("vpc_flow") { integration =>
      integration
        .createSourceTable(s"$catalogName.default.vpc_low_test")
        .createMaterializedView(mvQueryVPC, sourceDisabled = true)
        .assertDslQueryPieChart(dslQueryBuilderVPCPieChart, expectedBucketsVPCPieChart)
    }
  }

  test("create aggregated materialized view for VPC flow integration") {
    withIntegration("vpc_flow") { integration =>
      integration
        .createSourceTable(s"$catalogName.default.vpc_low_test")
        .createMaterializedView(mvQueryVPC, sourceDisabled = false)
        .assertIndexData(
          Row(
            timestampFromUTC("2023-11-01T05:00:00Z"),
            "ACCEPT",
            "10.0.0.1",
            "10.0.0.2",
            6,
            2,
            350.0,
            15),
          Row(
            timestampFromUTC("2023-11-01T05:10:00Z"),
            "ACCEPT",
            "10.0.0.3",
            "10.0.0.4",
            6,
            1,
            300.0,
            15),
          Row(
            timestampFromUTC("2023-11-01T05:10:00Z"),
            "REJECT",
            "10.0.0.5",
            "10.0.0.6",
            6,
            1,
            400.0,
            20))
    }
  }
}
