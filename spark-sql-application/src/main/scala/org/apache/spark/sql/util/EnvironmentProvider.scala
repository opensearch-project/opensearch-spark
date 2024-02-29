/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

/**
 * Trait defining an interface for fetching environment variables.
 */
trait EnvironmentProvider {

  /**
   * Retrieves the value of an environment variable.
   *
   * @param name
   *   The name of the environment variable.
   * @param default
   *   The default value to return if the environment variable is not set.
   * @return
   *   The value of the environment variable if it exists, otherwise the default value.
   */
  def getEnvVar(name: String, default: String): String
}
