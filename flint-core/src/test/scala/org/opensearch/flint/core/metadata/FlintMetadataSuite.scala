/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata

import scala.collection.JavaConverters.mapAsJavaMapConverter

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.core.FlintVersion.current
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FlintMetadataSuite extends AnyFlatSpec with Matchers {

  /** Test Flint index meta JSON string */
  val testMetadataJson: String = s"""
                | {
                |   "_meta": {
                |     "version": "${current()}",
                |     "name": "test_index",
                |     "kind": "test_kind",
                |     "source": "test_source_table",
                |     "indexedColumns": [
                |     {
                |       "test_field": "spark_type"
                |     }],
                |     "options": {},
                |     "properties": {}
                |   },
                |   "properties": {
                |     "test_field": {
                |       "type": "os_type"
                |     }
                |   }
                | }
                |""".stripMargin

  val testIndexSettingsJson: String =
    """
      | { "number_of_shards": 3 }
      |""".stripMargin

  "constructor" should "deserialize the given JSON and assign parsed value to field" in {
    val metadata = FlintMetadata.fromJson(testMetadataJson, testIndexSettingsJson)

    metadata.version shouldBe current()
    metadata.name shouldBe "test_index"
    metadata.kind shouldBe "test_kind"
    metadata.source shouldBe "test_source_table"
    metadata.indexedColumns shouldBe Map("test_field" -> "spark_type").asJava
    metadata.schema shouldBe Map("test_field" -> Map("type" -> "os_type").asJava).asJava
  }

  "getContent" should "serialize all fields to JSON" in {
    val metadata = new FlintMetadata.Builder
    metadata.name("test_index")
    metadata.kind("test_kind")
    metadata.source("test_source_table")
    metadata.addIndexedColumn("test_field", "spark_type");
    metadata.addSchemaField("test_field", Map("type" -> "os_type").asJava)

    metadata.build().getContent should matchJson(testMetadataJson)
  }
}
