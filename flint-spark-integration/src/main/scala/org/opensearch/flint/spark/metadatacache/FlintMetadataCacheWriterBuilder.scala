/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.metadatacache

import org.apache.spark.sql.flint.config.FlintSparkConf

object FlintMetadataCacheWriterBuilder {
  def build(flintSparkConf: FlintSparkConf): FlintMetadataCacheWriter = {
    if (flintSparkConf.isMetadataCacheWriteEnabled) {
      new FlintOpenSearchMetadataCacheWriter(flintSparkConf.flintOptions())
    } else {
      new FlintDisabledMetadataCacheWriter
    }
  }
}
