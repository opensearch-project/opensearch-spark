/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

/**
 * A trait allows injecting shutdown hook manager.
 */
trait ShutdownHookManagerTrait {
  def addShutdownHook(hook: () => Unit): AnyRef
}
