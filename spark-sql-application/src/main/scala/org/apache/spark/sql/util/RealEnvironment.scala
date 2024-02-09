/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

/**
 * An implementation of `EnvironmentProvider` that fetches actual environment variables from the
 * system.
 */
class RealEnvironment extends EnvironmentProvider {

  /**
   * Retrieves the value of an environment variable from the system or returns a default value if
   * not present.
   *
   * @param name
   *   The name of the environment variable.
   * @param default
   *   The default value to return if the environment variable is not set in the system.
   * @return
   *   The value of the environment variable if it exists in the system, otherwise the default
   *   value.
   */
  def getEnvVar(name: String, default: String): String = sys.env.getOrElse(name, default)
}
