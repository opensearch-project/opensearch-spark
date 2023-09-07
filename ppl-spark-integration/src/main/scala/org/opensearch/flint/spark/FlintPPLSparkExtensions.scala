/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.SparkSessionExtensions
import org.opensearch.flint.spark.ppl.FlintSparkPPLParser

/**
 * Flint PPL Spark extension entrypoint.
 */
class FlintPPLSparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (spark, parser) =>
      new FlintSparkPPLParser(parser)
    }
  }
}
