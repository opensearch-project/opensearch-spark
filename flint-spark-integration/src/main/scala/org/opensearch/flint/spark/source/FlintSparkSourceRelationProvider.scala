/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.source

import org.opensearch.flint.spark.source.file.FileSourceRelationProvider
import org.opensearch.flint.spark.source.iceberg.IcebergSourceRelationProvider

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * A provider defines what kind of logical plan can be supported by Flint Spark integration. It
 * serves similar purpose to Scala extractor which has to be used in match case statement.
 * However, the problem here is we want to avoid hard dependency on some data source code, such as
 * Iceberg. In this case, we have to maintain a list of provider and run it only if the 3rd party
 * library is available in current Spark session.
 */
trait FlintSparkSourceRelationProvider {

  /**
   * @return
   *   the name of the source relation provider
   */
  def name(): String

  /**
   * Determines whether the given logical plan is supported by this provider.
   *
   * @param plan
   *   the logical plan to evaluate
   * @return
   *   true if the plan is supported, false otherwise
   */
  def isSupported(plan: LogicalPlan): Boolean

  /**
   * Creates a source relation based on the provided logical plan.
   *
   * @param plan
   *   the logical plan to wrap in source relation
   * @return
   *   an instance of source relation
   */
  def getRelation(plan: LogicalPlan): FlintSparkSourceRelation
}

/**
 * Companion object provides utility methods.
 */
object FlintSparkSourceRelationProvider {

  /**
   * Retrieve all supported source relation provider for the given Spark session.
   *
   * @param spark
   *   the Spark session
   * @return
   *   a sequence of source relation provider
   */
  def getAllProviders(spark: SparkSession): Seq[FlintSparkSourceRelationProvider] = {
    var relations = Seq[FlintSparkSourceRelationProvider]()

    // File source is built-in supported
    relations = relations :+ new FileSourceRelationProvider

    // Add Iceberg provider if it's enabled in Spark conf
    if (spark.conf
        .getOption("spark.sql.catalog.spark_catalog")
        .contains("org.apache.iceberg.spark.SparkSessionCatalog")) {
      relations = relations :+ new IcebergSourceRelationProvider
    }
    relations
  }
}
