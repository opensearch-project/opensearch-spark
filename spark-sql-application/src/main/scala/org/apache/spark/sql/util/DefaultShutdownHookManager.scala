/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

import org.apache.spark.util.ShutdownHookManager

object DefaultShutdownHookManager extends ShutdownHookManagerTrait {
  override def addShutdownHook(hook: () => Unit): AnyRef = {
    ShutdownHookManager.addShutdownHook(hook)
  }
}
