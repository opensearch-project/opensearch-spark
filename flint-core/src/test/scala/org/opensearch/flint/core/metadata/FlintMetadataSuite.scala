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

  /** Test metadata JSON string */
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

  "constructor" should "deserialize the given JSON and assign parsed value to field" in {
    val metadata = new FlintMetadata(testMetadataJson)

    metadata.getVersion shouldBe current()
    metadata.getName shouldBe "test_index"
    metadata.getKind shouldBe "test_kind"
    metadata.getSource shouldBe "test_source_table"
    metadata.getIndexedColumns shouldBe Map("test_field" -> "spark_type").asJava
    metadata.getSchema shouldBe Map("test_field" -> Map("type" -> "os_type").asJava).asJava
  }

  "getContent" should "serialize all fields to JSON" in {
    val metadata = new FlintMetadata()
    metadata.setName("test_index")
    metadata.setKind("test_kind")
    metadata.setSource("test_source_table")
    metadata.getIndexedColumns.put("test_field", "spark_type");
    metadata.getSchema.put("test_field", Map("type" -> "os_type").asJava)
    // metadata.setIndexedColumns("""[{"test_field": "spark_type"}]""")
    // metadata.setSchema("""{"test_field": {"type": "os_type"}}""")

    metadata.getContent should matchJson(testMetadataJson)
  }
}
