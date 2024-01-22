/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

trait EnvironmentProvider {
  def getEnvVar(name: String, default: String): String
}
