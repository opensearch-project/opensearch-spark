/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.scheduler

import java.time.Instant

import org.opensearch.flint.common.scheduler.AsyncQueryScheduler
import org.opensearch.flint.common.scheduler.model.{AsyncQuerySchedulerRequest, LangType}
import org.opensearch.flint.spark.FlintSparkIndex
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
    with Logging {

  override def handleJob(index: FlintSparkIndex, action: AsyncQuerySchedulerAction): Unit = {
    val dataSource = flintSparkConf.flintOptions().getDataSourceName()
    val clientId = flintSparkConf.flintOptions().getAWSAccountId()
    val indexName = index.name()

    logInfo(s"handleAsyncQueryScheduler invoked: $action")

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
      case AsyncQuerySchedulerAction.UNSCHEDULE => flintAsyncQueryScheduler.unscheduleJob(request)
      case AsyncQuerySchedulerAction.REMOVE => flintAsyncQueryScheduler.removeJob(request)
      case _ => throw new IllegalArgumentException(s"Unsupported action: $action")
    }
  }
}
