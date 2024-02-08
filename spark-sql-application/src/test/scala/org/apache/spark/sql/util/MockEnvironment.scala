/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

/**
 * A mock implementation of `EnvironmentProvider` for use in tests, where environment variables
 * can be predefined.
 *
 * @param inputMap
 *   A map representing the environment variables (name -> value).
 */
class MockEnvironment(inputMap: Map[String, String]) extends EnvironmentProvider {

  /**
   * Retrieves the value of an environment variable from the input map or returns a default value
   * if not present.
   *
   * @param name
   *   The name of the environment variable.
   * @param default
   *   The default value to return if the environment variable is not set in the input map.
   * @return
   *   The value of the environment variable from the input map if it exists, otherwise the
   *   default value.
   */
  def getEnvVar(name: String, default: String): String = inputMap.getOrElse(name, default)
}
