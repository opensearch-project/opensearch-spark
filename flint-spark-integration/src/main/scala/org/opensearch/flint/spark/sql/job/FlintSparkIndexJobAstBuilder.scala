/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql.job

import org.opensearch.flint.spark.sql.{FlintSparkSqlCommand, FlintSparkSqlExtensionsVisitor, SparkSqlAstBuilder}
import org.opensearch.flint.spark.sql.FlintSparkSqlExtensionsParser.RecoverIndexJobStatementContext

import org.apache.spark.sql.catalyst.plans.logical.Command

/**
 * Flint Spark AST builder that builds Spark command for Flint index job management statement.
 */
trait FlintSparkIndexJobAstBuilder extends FlintSparkSqlExtensionsVisitor[AnyRef] {
  self: SparkSqlAstBuilder =>

  override def visitRecoverIndexJobStatement(ctx: RecoverIndexJobStatementContext): Command = {
    FlintSparkSqlCommand() { flint =>
      val jobId = ctx.identifier().getText
      flint.recoverIndex(jobId)
      Seq.empty
    }
  }
}
