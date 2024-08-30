/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

import org.apache.spark.sql.QueryMetadataService
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.util.Utils

case class CustomClassLoader(flintSparkConf: FlintSparkConf) {

  def getQueryMetadataService(): QueryMetadataService = {
    instantiateClass[QueryMetadataService](
      flintSparkConf.flintOptions().getCustomQueryMetadataService)
  }

  private def instantiateClass[T](className: String): T = {
    try {
      val providerClass = Utils.classForName(className)
      val ctor = providerClass.getDeclaredConstructor(classOf[FlintSparkConf])
      ctor.setAccessible(true)
      ctor.newInstance(flintSparkConf).asInstanceOf[T]
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to instantiate provider: $className", e)
    }
  }
}
