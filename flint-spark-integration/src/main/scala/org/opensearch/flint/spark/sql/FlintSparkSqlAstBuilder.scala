/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql

import org.opensearch.flint.spark.sql.skipping.FlintSparkSqlSkippingIndexAstBuilder

import org.apache.spark.sql.catalyst.plans.logical.Command

/**
 * Flint Spark AST builder that builds Spark command for Flint index statement.
 */
class FlintSparkSqlAstBuilder extends FlintSparkSqlSkippingIndexAstBuilder {

  override def aggregateResult(aggregate: Command, nextResult: Command): Command =
    if (nextResult != null) nextResult else aggregate
}
