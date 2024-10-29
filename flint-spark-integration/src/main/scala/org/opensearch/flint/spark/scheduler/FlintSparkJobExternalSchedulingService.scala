/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.scheduler

import java.time.Instant

import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState
import org.opensearch.flint.common.scheduler.AsyncQueryScheduler
import org.opensearch.flint.common.scheduler.model.{AsyncQuerySchedulerRequest, LangType}
import org.opensearch.flint.core.metrics.{MetricConstants, MetricsUtil}
import org.opensearch.flint.core.storage.OpenSearchClientUtils
import org.opensearch.flint.spark.FlintSparkIndex
import org.opensearch.flint.spark.refresh.util.RefreshMetricsAspect
import org.opensearch.flint.spark.scheduler.AsyncQuerySchedulerBuilder.AsyncQuerySchedulerAction
import org.opensearch.flint.spark.scheduler.util.RefreshQueryGenerator

import org.apache.spark.internal.Logging
import org.apache.spark.sql.flint.config.FlintSparkConf

/**
 * External scheduling service for Flint Spark jobs.
 *
 * This class implements the FlintSparkJobSchedulingService interface and provides functionality
 * to handle job scheduling, updating, unscheduling, and removal using an external
 * AsyncQueryScheduler.
 *
 * @param flintAsyncQueryScheduler
 *   The AsyncQueryScheduler used for job management
 * @param flintSparkConf
 *   The Flint Spark configuration
 */
class FlintSparkJobExternalSchedulingService(
    flintAsyncQueryScheduler: AsyncQueryScheduler,
    flintSparkConf: FlintSparkConf)
    extends FlintSparkJobSchedulingService
    with RefreshMetricsAspect
    with Logging {

  override val stateTransitions: StateTransitions = StateTransitions(
    initialStateForUpdate = IndexState.ACTIVE,
    finalStateForUpdate = IndexState.ACTIVE,
    initialStateForUnschedule = IndexState.ACTIVE,
    finalStateForUnschedule = IndexState.ACTIVE)

  override def handleJob(
      index: FlintSparkIndex,
      action: AsyncQuerySchedulerAction): Option[String] = {
    val dataSource = flintSparkConf.flintOptions().getDataSourceName()
    val clientId = flintSparkConf.flintOptions().getAWSAccountId()
    // This is to make sure jobId is consistent with the index name
    val indexName = OpenSearchClientUtils.sanitizeIndexName(index.name())

    logInfo(s"handleAsyncQueryScheduler invoked: $action")

    withMetrics(clientId, dataSource, indexName, "externalScheduler") {
      val baseRequest = AsyncQuerySchedulerRequest
        .builder()
        .accountId(clientId)
        .jobId(indexName)
        .dataSource(dataSource)

      val request = action match {
        case AsyncQuerySchedulerAction.SCHEDULE | AsyncQuerySchedulerAction.UPDATE =>
          val currentTime = Instant.now()
          baseRequest
            .scheduledQuery(RefreshQueryGenerator.generateRefreshQuery(index))
            .queryLang(LangType.SQL)
            .interval(index.options.refreshInterval().get)
            .enabled(true)
            .enabledTime(currentTime)
            .lastUpdateTime(currentTime)
            .build()
        case _ => baseRequest.build()
      }

      action match {
        case AsyncQuerySchedulerAction.SCHEDULE => flintAsyncQueryScheduler.scheduleJob(request)
        case AsyncQuerySchedulerAction.UPDATE => flintAsyncQueryScheduler.updateJob(request)
        case AsyncQuerySchedulerAction.UNSCHEDULE =>
          flintAsyncQueryScheduler.unscheduleJob(request)
        case AsyncQuerySchedulerAction.REMOVE => flintAsyncQueryScheduler.removeJob(request)
        case _ => throw new IllegalArgumentException(s"Unsupported action: $action")
      }
      addExternalSchedulerMetrics(action)

      None // Return None for all cases
    }
  }

  private def addExternalSchedulerMetrics(action: AsyncQuerySchedulerAction): Unit = {
    val actionName = action.name().toLowerCase()
    MetricsUtil.addHistoricGauge(
      MetricConstants.EXTERNAL_SCHEDULER_METRIC_PREFIX + actionName + ".count",
      1)
  }
}
