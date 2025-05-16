/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.aggregations.BucketOrder
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.opensearch.search.builder.SearchSourceBuilder

import org.apache.spark.sql.Row

class FlintSparkMaterializedViewIntegrationWAFITSuite
    extends FlintSparkMaterializedViewIntegrationsITSuite {

  val mvQueryWAF: String = s"""
                              |SELECT
                              |  TUMBLE(`@timestamp`, '5 Minute').start AS `start_time`,
                              |  webaclId AS `aws.waf.webaclId`,
                              |  action AS `aws.waf.action`,
                              |  `httpRequest.clientIp` AS `aws.waf.httpRequest.clientIp`,
                              |  `httpRequest.country` AS `aws.waf.httpRequest.country`,
                              |  `httpRequest.uri` AS `aws.waf.httpRequest.uri`,
                              |  `httpRequest.httpMethod` AS `aws.waf.httpRequest.httpMethod`,
                              |  httpSourceId AS `aws.waf.httpSourceId`,
                              |  terminatingRuleId AS `aws.waf.terminatingRuleId`,
                              |  terminatingRuleType AS `aws.waf.RuleType`,
                              |  `ruleGroupList.ruleId` AS `aws.waf.ruleGroupList.ruleId`,
                              |  COUNT(*) AS `aws.waf.event_count`
                              |FROM (
                              |  SELECT
                              |    CAST(FROM_UNIXTIME(`timestamp`/1000) AS TIMESTAMP) AS `@timestamp`,
                              |    webaclId,
                              |    action,
                              |    httpRequest.clientIp AS `httpRequest.clientIp`,
                              |    httpRequest.country AS `httpRequest.country`,
                              |    httpRequest.uri AS `httpRequest.uri`,
                              |    httpRequest.httpMethod AS `httpRequest.httpMethod`,
                              |    httpSourceId,
                              |    terminatingRuleId,
                              |    terminatingRuleType,
                              |    ruleGroupList.ruleId AS `ruleGroupList.ruleId`
                              |  FROM
                              |    $catalogName.default.waf_test
                              |)
                              |GROUP BY
                              |  TUMBLE(`@timestamp`, '5 Minute'),
                              |  webaclId,
                              |  action,
                              |  `httpRequest.clientIp`,
                              |  `httpRequest.country`,
                              |  `httpRequest.uri`,
                              |  `httpRequest.httpMethod`,
                              |  httpSourceId,
                              |  terminatingRuleId,
                              |  terminatingRuleType,
                              |  `ruleGroupList.ruleId`
                              |""".stripMargin

  val dslQueryBuilderWAFTimeSeriesChart: SearchSourceBuilder = {
    val builder = new SearchSourceBuilder()
      .query(QueryBuilders.wrapperQuery(dslQueryString))

    val termsAgg = AggregationBuilders
      .terms("2")
      .field("aws.waf.action")
      .size(5)
      .order(BucketOrder.aggregation("1", false))

    val sumAgg = AggregationBuilders
      .sum("1")
      .field("aws.waf.event_count")

    val dateHistogramAgg = AggregationBuilders
      .dateHistogram("3")
      .field("start_time")
      .fixedInterval(new DateHistogramInterval("30s"))
      .minDocCount(1)

    val innerSumAgg = AggregationBuilders
      .sum("1")
      .field("aws.waf.event_count")

    dateHistogramAgg.subAggregation(innerSumAgg)
    termsAgg.subAggregation(sumAgg)
    termsAgg.subAggregation(dateHistogramAgg)

    builder.aggregation(termsAgg)

    builder
  }

  val dslQueryBuilderWAFPieChart: SearchSourceBuilder = {
    val builder = new SearchSourceBuilder()
      .query(QueryBuilders.wrapperQuery(dslQueryString))
    val termsAgg = AggregationBuilders
      .terms("2")
      .field("aws.waf.httpRequest.clientIp")
      .order(BucketOrder.aggregation("1", false))

    val sumAgg = AggregationBuilders
      .sum("1")
      .field("aws.waf.event_count")

    termsAgg.subAggregation(sumAgg)
    builder.aggregation(termsAgg)

    builder
  }

  val expectedBucketsWAFTimeSeriesChart: Seq[Map[String, Any]] = Seq(
    Map("key_as_string" -> "2023-11-01T05:00:00.000Z", "doc_count" -> 1))

  val expectedBucketsWAFPieChart: Seq[Map[String, Any]] = Seq(
    Map("key" -> "192.0.2.1", "doc_count" -> 1))

  test("create aggregated materialized view for WAF integration unfiltered time series chart") {
    withIntegration("waf") { integration =>
      integration
        .createSourceTable(s"$catalogName.default.waf_test")
        .createMaterializedView(mvQueryWAF, sourceDisabled = true)
        .assertDslQueryWAFTimeSeriesChart(
          dslQueryBuilderWAFTimeSeriesChart,
          expectedBucketsWAFTimeSeriesChart)
    }
  }

  test("create aggregated materialized view for WAF integration unfiltered pie chart") {
    withIntegration("waf") { integration =>
      integration
        .createSourceTable(s"$catalogName.default.waf_test")
        .createMaterializedView(mvQueryWAF, sourceDisabled = true)
        .assertDslQueryPieChart(dslQueryBuilderWAFPieChart, expectedBucketsWAFPieChart)
    }
  }

  test("create aggregated materialized view for WAF integration") {
    withIntegration("waf") { integration =>
      integration
        .createSourceTable(s"$catalogName.default.waf_test")
        .createMaterializedView(mvQueryWAF, sourceDisabled = false)
        .assertIndexData(Row(
          timestampFromUTC("2023-11-01T05:00:00Z"),
          "webacl-12345",
          "ALLOW",
          "192.0.2.1",
          "US",
          "/index.html",
          "GET",
          "source-1",
          "rule-1",
          "REGULAR",
          Array("group-rule-1"),
          1))
    }
  }
}
