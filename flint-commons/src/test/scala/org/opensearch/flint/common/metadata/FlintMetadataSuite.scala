/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.common.metadata

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.common.FlintVersion.current
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FlintMetadataSuite extends AnyFlatSpec with Matchers {
  "builder" should "build FlintMetadata with provided fields" in {
    val builder = new FlintMetadata.Builder
    builder.name("test_index")
    builder.kind("test_kind")
    builder.source("test_source_table")
    builder.addIndexedColumn(Map[String, AnyRef]("test_field" -> "spark_type").asJava)
    builder.schema(Map[String, AnyRef]("test_field" -> Map("type" -> "os_type").asJava).asJava)

    val metadata = builder.build()

    metadata.version shouldBe current()
    metadata.name shouldBe "test_index"
    metadata.kind shouldBe "test_kind"
    metadata.source shouldBe "test_source_table"
    metadata.indexedColumns shouldBe Array(Map("test_field" -> "spark_type").asJava)
    metadata.schema shouldBe Map("test_field" -> Map("type" -> "os_type").asJava).asJava
  }
}
