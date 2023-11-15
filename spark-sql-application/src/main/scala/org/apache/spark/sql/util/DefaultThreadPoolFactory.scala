/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

import java.util.concurrent.ScheduledExecutorService

import org.apache.spark.util.ThreadUtils

class DefaultThreadPoolFactory extends ThreadPoolFactory {
  override def newDaemonThreadPoolScheduledExecutor(
      threadNamePrefix: String,
      numThreads: Int): ScheduledExecutorService = {
    ThreadUtils.newDaemonThreadPoolScheduledExecutor(threadNamePrefix, numThreads)
  }
}
