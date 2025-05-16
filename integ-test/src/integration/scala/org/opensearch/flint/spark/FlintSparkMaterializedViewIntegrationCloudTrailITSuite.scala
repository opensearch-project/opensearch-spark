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

class FlintSparkMaterializedViewIntegrationCloudTrailITSuite
    extends FlintSparkMaterializedViewIntegrationsITSuite {

  val mvQueryCT: String = s"""
                             |SELECT
                             |  TUMBLE(`@timestamp`, '5 Minute').start AS `start_time`,
                             |  `userIdentity.type` AS `aws.cloudtrail.userIdentity.type`,
                             |  `userIdentity.accountId` AS `aws.cloudtrail.userIdentity.accountId`,
                             |  `userIdentity.sessionContext.sessionIssuer.userName` AS `aws.cloudtrail.userIdentity.sessionContext.sessionIssuer.userName`,
                             |  `userIdentity.sessionContext.sessionIssuer.arn` AS `aws.cloudtrail.userIdentity.sessionContext.sessionIssuer.arn`,
                             |  `userIdentity.sessionContext.sessionIssuer.type` AS `aws.cloudtrail.userIdentity.sessionContext.sessionIssuer.type`,
                             |  awsRegion AS `aws.cloudtrail.awsRegion`,
                             |  sourceIPAddress AS `aws.cloudtrail.sourceIPAddress`,
                             |  eventSource AS `aws.cloudtrail.eventSource`,
                             |  eventName AS `aws.cloudtrail.eventName`,
                             |  eventCategory AS `aws.cloudtrail.eventCategory`,
                             |  COUNT(*) AS `aws.cloudtrail.event_count`
                             |FROM (
                             |  SELECT
                             |    CAST(eventTime AS TIMESTAMP) AS `@timestamp`,
                             |    userIdentity.`type` AS `userIdentity.type`,
                             |    userIdentity.`accountId` AS `userIdentity.accountId`,
                             |    userIdentity.sessionContext.sessionIssuer.userName AS `userIdentity.sessionContext.sessionIssuer.userName`,
                             |    userIdentity.sessionContext.sessionIssuer.arn AS `userIdentity.sessionContext.sessionIssuer.arn`,
                             |    userIdentity.sessionContext.sessionIssuer.type AS `userIdentity.sessionContext.sessionIssuer.type`,
                             |    awsRegion,
                             |    sourceIPAddress,
                             |    eventSource,
                             |    eventName,
                             |    eventCategory
                             |  FROM
                             |    $catalogName.default.cloud_trail_test
                             |)
                             |GROUP BY
                             |  TUMBLE(`@timestamp`, '5 Minute'),
                             |  `userIdentity.type`,
                             |  `userIdentity.accountId`,
                             |  `userIdentity.sessionContext.sessionIssuer.userName`,
                             |  `userIdentity.sessionContext.sessionIssuer.arn`,
                             |  `userIdentity.sessionContext.sessionIssuer.type`,
                             |  awsRegion,
                             |  sourceIPAddress,
                             |  eventSource,
                             |  eventName,
                             |  eventCategory
                             |""".stripMargin

  val dslQueryBuilderCTTimeSeriesChart: SearchSourceBuilder = {
    val builder = new SearchSourceBuilder()
      .query(QueryBuilders.wrapperQuery(dslQueryString))
    val dateHistogramAgg = AggregationBuilders
      .dateHistogram("2")
      .field("start_time")
      .fixedInterval(DateHistogramInterval.seconds(30))

    val sumAgg = AggregationBuilders
      .sum("1")
      .field("aws.cloudtrail.event_count")

    dateHistogramAgg.subAggregation(sumAgg)
    builder.aggregation(dateHistogramAgg)

    builder
  }

  val dslQueryBuilderCTPieChart: SearchSourceBuilder = {
    val builder = new SearchSourceBuilder()
      .query(QueryBuilders.wrapperQuery(dslQueryString))
    val termsAgg = AggregationBuilders
      .terms("2")
      .field("aws.cloudtrail.sourceIPAddress")
      .order(BucketOrder.aggregation("1", false))

    val sumAgg = AggregationBuilders
      .sum("1")
      .field("aws.cloudtrail.event_count")

    termsAgg.subAggregation(sumAgg)
    builder.aggregation(termsAgg)

    builder
  }

  val expectedBucketsCTTimeSeriesChart: Seq[Map[String, Any]] = Seq(
    Map("key_as_string" -> "2023-11-01T05:00:00.000Z", "doc_count" -> 1))

  val expectedBucketsCTPieChart: Seq[Map[String, Any]] = Seq(
    Map("key" -> "198.51.100.45", "doc_count" -> 1))

  test(
    "create aggregated materialized view for CloudTrail integration unfiltered time series chart") {
    withIntegration("cloud_trail") { integration =>
      integration
        .createSourceTable(s"$catalogName.default.cloud_trail_test")
        .createMaterializedView(mvQueryCT, sourceDisabled = true)
        .assertDslQueryTimeSeriesChart(
          dslQueryBuilderCTTimeSeriesChart,
          expectedBucketsCTTimeSeriesChart)
    }
  }

  test("create aggregated materialized view for CloudTrail integration unfiltered pie chart") {
    withIntegration("cloud_trail") { integration =>
      integration
        .createSourceTable(s"$catalogName.default.cloud_trail_test")
        .createMaterializedView(mvQueryCT, sourceDisabled = true)
        .assertDslQueryPieChart(dslQueryBuilderCTPieChart, expectedBucketsCTPieChart)
    }
  }

  test("create aggregated materialized view for CloudTrail integration") {
    withIntegration("cloud_trail") { integration =>
      integration
        .createSourceTable(s"$catalogName.default.cloud_trail_test")
        .createMaterializedView(mvQueryCT, sourceDisabled = false)
        .assertIndexData(Row(
          timestampFromUTC("2023-11-01T05:00:00Z"),
          "IAMUser",
          "123456789012",
          "MyRole",
          "arn:aws:iam::123456789012:role/MyRole",
          "Role",
          "us-east-1",
          "198.51.100.45",
          "sts.amazonaws.com",
          "AssumeRole",
          "Management",
          1))
    }
  }
}
