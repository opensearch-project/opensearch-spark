/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

class MockEnvironment(inputMap: Map[String, String]) extends EnvironmentProvider {
  def getEnvVar(name: String, default: String): String = inputMap.getOrElse(name, default)
}
