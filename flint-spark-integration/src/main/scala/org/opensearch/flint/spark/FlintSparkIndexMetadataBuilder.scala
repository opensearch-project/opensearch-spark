/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.json4s.JObject
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.FlintSparkIndex.populateEnvToMetadata

import org.apache.spark.sql.flint.datatype.FlintDataType
import org.apache.spark.sql.types.StructType

/**
 * Flint Spark metadata builder with common build logic.
 */
class FlintSparkIndexMetadataBuilder(index: FlintSparkIndex) extends FlintMetadata.Builder {

  def schema(allFieldTypes: Map[String, String]): FlintSparkIndexMetadataBuilder = {
    val catalogDDL =
      allFieldTypes
        .map { case (colName, colType) => s"$colName $colType not null" }
        .mkString(",")
    val structType = StructType.fromDDL(catalogDDL)
    val properties = FlintDataType.serialize(structType)
    schema(properties)
    this
  }

  override def build(): FlintMetadata = {
    // Common fields in all Flint Spark index
    kind(index.kind)
    name(index.name())
    options(index.options.options.mapValues(_.asInstanceOf[AnyRef]).asJava)

    val envs = populateEnvToMetadata
    if (envs.nonEmpty) {
      addProperty("env", envs.asJava)
    }

    val settings = index.options.indexSettings()
    if (settings.isDefined) {
      indexSettings(settings.get)
    }
    super.build()
  }
}

object FlintSparkIndexMetadataBuilder {

  def builder(index: FlintSparkIndex): FlintSparkIndexMetadataBuilder =
    new FlintSparkIndexMetadataBuilder(index)
}
