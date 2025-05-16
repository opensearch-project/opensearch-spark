/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.io.File
import java.sql.Timestamp
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import scala.jdk.CollectionConverters.asScalaBufferConverter

import org.opensearch.action.search.SearchRequest
import org.opensearch.client.RequestOptions
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.getFlintIndexName
import org.opensearch.search.aggregations.bucket.histogram.ParsedDateHistogram
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms
import org.opensearch.search.builder.SearchSourceBuilder
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE

/**
 * Base class for AWS integrations with Flint Materialized Views.
 */
abstract class FlintSparkMaterializedViewIntegrationsITSuite
    extends FlintSparkSuite
    with Matchers {

  val dslQueryString: String =
    """
      |{
      |    "bool": {
      |      "filter": [
      |        {
      |          "match_all": {}
      |        }
      |      ]
      |    }
      |}
      |""".stripMargin

  /**
   * Executes a block of code within the context of a specific integration test.
   *
   * @param name
   *   the name of the integration (e.g., "waf", "cloud_trail")
   * @param codeBlock
   *   the block of code to execute with the integration setup
   */
  def withIntegration(name: String)(codeBlock: IntegrationHelper => Unit): Unit = {
    withTempDir { checkpointDir =>
      val integration = new IntegrationHelper(name, checkpointDir)
      try {
        codeBlock(integration)
      } finally {
        sql(s"DROP TABLE ${integration.tableName}")
        val indexName = integration.getIndexName
        if (indexName != null) {
          deleteTestIndex(indexName)
        }
      }
    }
  }

  /**
   * A helper class to facilitate actions like creating source tables, materialized views, and
   * asserting results.
   */
  class IntegrationHelper(integrationName: String, checkpointDir: File) {
    var tableName: String = _
    private var mvName: String = _
    private var mvQuery: String = _

    def getIndexName: String = {
      if (mvName != null) {
        org.opensearch.flint.spark.mv.FlintSparkMaterializedView.getFlintIndexName(mvName)
      } else {
        null
      }
    }

    def createSourceTable(tableName: String): IntegrationHelper = {
      this.tableName = tableName
      val sqlTemplate = resourceToString(s"aws-logs/$integrationName.sql").mkString
      val sqlStatements =
        sqlTemplate
          .replace("{table_name}", tableName)
          .split(';')
          .map(_.trim)
          .filter(_.nonEmpty)

      sqlStatements.foreach(spark.sql)
      this
    }

    def createMaterializedView(mvQuery: String, sourceDisabled: Boolean): IntegrationHelper = {
      this.mvName = s"$catalogName.default.${integrationName}_mv_test"
      this.mvQuery = mvQuery.replace("{table_name}", tableName)

      if (sourceDisabled) {
        sql(s"""
               |CREATE MATERIALIZED VIEW $mvName
               |AS
               |${this.mvQuery}
               |WITH (
               |  auto_refresh = true,
               |  refresh_interval = '5 Seconds',
               |  watermark_delay = '1 Minute',
               |  checkpoint_location = '${checkpointDir.getAbsolutePath}',
               |  index_mappings = '{ "_source": { "enabled": false } }'
               |)
               |""".stripMargin)
      } else {
        sql(s"""
               |CREATE MATERIALIZED VIEW $mvName
               |AS
               |${this.mvQuery}
               |WITH (
               |  auto_refresh = true,
               |  refresh_interval = '5 Seconds',
               |  watermark_delay = '1 Minute',
               |  checkpoint_location = '${checkpointDir.getAbsolutePath}'
               |)
               |""".stripMargin)
      }

      // Wait for all data processed
      val job = spark.streams.active
        .find(_.name == getFlintIndexName(mvName))
        .getOrElse(fail(s"Streaming job not found for integration: $integrationName"))
      failAfter(streamingTimeout) {
        job.processAllAvailable()
      }
      this
    }

    def assertIndexData(expectedRows: Row*): Unit = {
      val flintIndexName =
        spark.streams.active.find(_.name == getFlintIndexName(mvName)).get.name
      val actualRows = spark.read
        .format(FLINT_DATASOURCE)
        .options(openSearchOptions)
        .schema(sql(mvQuery).schema)
        .load(flintIndexName)

      checkAnswer(actualRows, expectedRows)
    }

    def assertDslQueryTimeSeriesChart(
        dslQueryTSC: SearchSourceBuilder,
        expectedBuckets: Seq[Map[String, Any]]): Unit = {
      val flintIndexName =
        spark.streams.active.find(_.name == getFlintIndexName(mvName)).get.name
      val searchRequest = new SearchRequest(flintIndexName)
      searchRequest.source(dslQueryTSC)
      val actual = openSearchClient.search(searchRequest, RequestOptions.DEFAULT)
      val dateHistogram = actual.getAggregations
        .get("2")
        .asInstanceOf[ParsedDateHistogram]

      val buckets = dateHistogram.getBuckets

      val actualBuckets = buckets.asScala.map { bucket =>
        Map("key_as_string" -> bucket.getKeyAsString, "doc_count" -> bucket.getDocCount.toInt)
      }

      actualBuckets should equal(expectedBuckets)
    }

    def assertDslQueryWAFTimeSeriesChart(
        dslQueryTSC: SearchSourceBuilder,
        expectedBuckets: Seq[Map[String, Any]]): Unit = {
      val flintIndexName =
        spark.streams.active.find(_.name == getFlintIndexName(mvName)).get.name
      val searchRequest = new SearchRequest(flintIndexName)
      searchRequest.source(dslQueryTSC)
      val actual = openSearchClient.search(searchRequest, RequestOptions.DEFAULT)
      val termsAgg = actual.getAggregations
        .get("2")
        .asInstanceOf[ParsedStringTerms]

      val firstTermBucket = termsAgg.getBuckets.asScala.head

      val dateHistogram = firstTermBucket.getAggregations
        .get("3")
        .asInstanceOf[ParsedDateHistogram]

      val buckets = dateHistogram.getBuckets

      val actualBuckets = buckets.asScala.map { bucket =>
        Map("key_as_string" -> bucket.getKeyAsString, "doc_count" -> bucket.getDocCount.toInt)
      }

      actualBuckets should equal(expectedBuckets)
    }

    def assertDslQueryPieChart(
        dslQueryBuilder: SearchSourceBuilder,
        expectedBuckets: Seq[Map[String, Any]]): Unit = {
      val flintIndexName =
        spark.streams.active.find(_.name == getFlintIndexName(mvName)).get.name
      val searchRequest = new SearchRequest(flintIndexName)
      searchRequest.source(dslQueryBuilder)
      val actual = openSearchClient.search(searchRequest, RequestOptions.DEFAULT)
      val stringTerms = actual.getAggregations
        .get("2")
        .asInstanceOf[ParsedStringTerms]

      val buckets = stringTerms.getBuckets

      val actualBuckets = buckets.asScala.map { bucket =>
        Map("key" -> bucket.getKeyAsString, "doc_count" -> bucket.getDocCount.toInt)
      }

      actualBuckets should equal(expectedBuckets)
    }
  }

  protected def timestampFromUTC(utcString: String): Timestamp = {
    val instant = ZonedDateTime.parse(utcString, DateTimeFormatter.ISO_DATE_TIME).toInstant
    Timestamp.from(instant)
  }
}
