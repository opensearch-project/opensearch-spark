/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.recommendations

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Recommendation rules for skipping index column and algorithm selection.
 */
class RecommendationRules {

  /** Recommendation config file */
  private val config = "skipping_index_recommendation.conf"

  /** Static rules for recommendations */
  private val rules: Config = ConfigFactory.load(config)

  /** Set of supported data types */
  private val dataTypes = rules.getConfig("recommendation.data_type_rules").root().keySet()

  /** Set of supported functions */
  private val functions = rules.getConfig("recommendation.function_rules").root().keySet()

  /**
   * Get skipping type recommendation.
   *
   * @param input
   *   input for recommendation can be a table partition or data type.
   * @return
   *   Skipping type
   */
  def getSkippingType(input: String): String = {
    if (input.equalsIgnoreCase("PARTITION")) {
      rules.getString("recommendation.table_partition.PARTITION.skipping_type")
    } else if (dataTypes.contains(input)) {
      rules.getString("recommendation.data_type_rules." + input + ".skipping_type")
    } else {
      null
    }
  }

  /**
   * Get reason for recommendation.
   *
   * @param input
   *   input for recommendation can be a table partition or data type.
   * @return
   *   reason for recommendation
   */
  def getReason(input: String): String = {
    if (input.equalsIgnoreCase("PARTITION")) {
      rules.getString("recommendation.table_partition.PARTITION.reason")
    } else if (dataTypes.contains(input)) {
      rules.getString("recommendation.data_type_rules." + input + ".reason")
    } else {
      null
    }

  }

  /**
   * Check if recommendation rule exists
   *
   * @param input
   *   input for recommendation can be a table partition or data type.
   * @return
   *   true if recommendation rule exists
   */
  def containsRule(input: String): Boolean = {
    if (dataTypes.contains(input)) {
      rules.hasPath("recommendation.data_type_rules." + input)
    } else if (functions.contains(input)) {
      rules.hasPath("recommendation.function_rules." + input)
    } else {
      false
    }
  }

}
