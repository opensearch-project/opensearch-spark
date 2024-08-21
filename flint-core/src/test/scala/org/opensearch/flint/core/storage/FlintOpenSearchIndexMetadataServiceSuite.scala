/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage

import scala.collection.JavaConverters.mapAsJavaMapConverter

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.common.FlintVersion.current
import org.opensearch.flint.common.metadata.FlintMetadata
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FlintOpenSearchIndexMetadataServiceSuite extends AnyFlatSpec with Matchers {

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

  val testDynamic: String = s"""
                | {
                |   "dynamic": "strict",
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

  val testNoSpec: String = s"""
                | {
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
    Seq(testMetadataJson, testDynamic).foreach(mapping => {
      val metadata =
        FlintOpenSearchIndexMetadataService.deserialize(mapping, testIndexSettingsJson)
      metadata.version shouldBe current()
      metadata.name shouldBe "test_index"
      metadata.kind shouldBe "test_kind"
      metadata.source shouldBe "test_source_table"
      metadata.indexedColumns shouldBe Array(Map("test_field" -> "spark_type").asJava)
      metadata.schema shouldBe Map("test_field" -> Map("type" -> "os_type").asJava).asJava
    })
  }

  "serialize" should "serialize all fields to JSON" in {
    val builder = new FlintMetadata.Builder
    builder.name("test_index")
    builder.kind("test_kind")
    builder.source("test_source_table")
    builder.addIndexedColumn(Map[String, AnyRef]("test_field" -> "spark_type").asJava)
    builder.schema(Map[String, AnyRef]("test_field" -> Map("type" -> "os_type").asJava).asJava)

    val metadata = builder.build()
    FlintOpenSearchIndexMetadataService.serialize(metadata) should matchJson(testMetadataJson)
  }

  "serialize without spec" should "serialize all fields to JSON without adding _meta field" in {
    val builder = new FlintMetadata.Builder
    builder.name("test_index")
    builder.kind("test_kind")
    builder.source("test_source_table")
    builder.addIndexedColumn(Map[String, AnyRef]("test_field" -> "spark_type").asJava)
    builder.schema(Map[String, AnyRef]("test_field" -> Map("type" -> "os_type").asJava).asJava)

    val metadata = builder.build()
    FlintOpenSearchIndexMetadataService.serialize(metadata, false) should matchJson(testNoSpec)
  }
}
