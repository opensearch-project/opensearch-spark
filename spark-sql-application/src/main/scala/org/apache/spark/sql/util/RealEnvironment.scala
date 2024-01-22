/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

class RealEnvironment extends EnvironmentProvider {
  def getEnvVar(name: String, default: String): String = sys.env.getOrElse(name, default)
}
