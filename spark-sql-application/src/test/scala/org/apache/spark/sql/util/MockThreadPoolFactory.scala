/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

import java.util.concurrent.ScheduledExecutorService

class MockThreadPoolFactory(mockExecutor: ScheduledExecutorService) extends ThreadPoolFactory {
  override def newDaemonThreadPoolScheduledExecutor(
      threadNamePrefix: String,
      numThreads: Int): ScheduledExecutorService = mockExecutor
}
