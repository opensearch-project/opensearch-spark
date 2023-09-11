/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.SparkSessionExtensions
import org.opensearch.flint.spark.ppl.FlintSparkPPLParser
import org.opensearch.flint.spark.sql.FlintSparkSqlParser

/**
 * Flint Spark extension entrypoint.
 */
class FlintGenericSparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (spark, parser) =>
      new FlintSparkParserChain(parser,Seq(new FlintSparkPPLParser(parser),new FlintSparkSqlParser(parser)))
    }
    extensions.injectOptimizerRule { spark =>
      new FlintSparkOptimizer(spark)
    }
  }
}
