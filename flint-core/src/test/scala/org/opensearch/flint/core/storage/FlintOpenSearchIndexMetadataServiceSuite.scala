/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage

import java.util

import scala.collection.JavaConverters.mapAsJavaMapConverter

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.common.FlintVersion.current
import org.opensearch.flint.common.metadata.FlintMetadata
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FlintOpenSearchIndexMetadataServiceSuite extends AnyFlatSpec with Matchers {

  /** Test Flint index meta JSON string */
  val testMetadataJsonWithoutIndexMapping: String = s"""
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

  val testMetadataJsonWithIndexMappingSourceEnabledFalse: String = s"""
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
                |     "options": {
                |       "index_mappings": "{ \\"_source\\": { \\"enabled\\": false } }"
                |     },
                |     "properties": {}
                |   },
                |   "_source": {
                |     "enabled": false
                |   },
                |   "properties": {
                |     "test_field": {
                |       "type": "os_type"
                |     }
                |   }
                | }
                |""".stripMargin

  val testMetadataJsonWithIndexMappingSourceEnabledTrue: String = s"""
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
                |     "options": {
                |       "index_mappings": "{ \\"_source\\": { \\"enabled\\": true } }"
                |     },
                |     "properties": {}
                |   },
                |   "properties": {
                |     "test_field": {
                |       "type": "os_type"
                |     }
                |   }
                | }
                |""".stripMargin

  val testMetadataJsonOtherThanSource: String = s"""
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
                |     "options": {
                |       "index_mappings": "{ \\"boost\\": 1.0 }"
                |     },
                |     "properties": {}
                |   },
                |   "properties": {
                |     "test_field": {
                |       "type": "os_type"
                |     }
                |   }
                | }
                |""".stripMargin

  val testMetadataJsonOtherThanEnabled: String = s"""
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
                |     "options": {
                |       "index_mappings": "{ \\"_source\\": { \\"includes\\": \\"meta.*\\" } }"
                |     },
                |     "properties": {}
                |   },
                |   "properties": {
                |     "test_field": {
                |       "type": "os_type"
                |     }
                |   }
                | }
                |""".stripMargin

  val testMetadataJsonWithSourceEnabledFalseAndSchemaMerging: String = s"""
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
                |     "options": {
                |       "index_mappings": "{ \\"_source\\": { \\"enabled\\": false }, \\"properties\\": { \\"test_field\\": {\\"index\\": false} } }"
                |     },
                |     "properties": {}
                |   },
                |   "_source": {
                |     "enabled": false
                |   },
                |   "properties": {
                |     "test_field": {
                |       "type": "os_type",
                |       "index": false
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
    Seq(testMetadataJsonWithoutIndexMapping, testDynamic).foreach(mapping => {
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

  "serialize" should "serialize all fields (no _source) to JSON" in {
    val builder = new FlintMetadata.Builder
    builder.name("test_index")
    builder.kind("test_kind")
    builder.source("test_source_table")
    builder.addIndexedColumn(Map[String, AnyRef]("test_field" -> "spark_type").asJava)
    builder.schema(Map[String, AnyRef]("test_field" -> Map("type" -> "os_type").asJava).asJava)

    val metadata = builder.build()
    FlintOpenSearchIndexMetadataService.serialize(metadata) should matchJson(
      testMetadataJsonWithoutIndexMapping)
  }

  "serialize" should "serialize all fields (include _source false) to JSON" in {
    val builder = new FlintMetadata.Builder
    builder.name("test_index")
    builder.kind("test_kind")
    builder.source("test_source_table")
    builder.addIndexedColumn(Map[String, AnyRef]("test_field" -> "spark_type").asJava)
    builder.schema(Map[String, AnyRef]("test_field" -> Map("type" -> "os_type").asJava).asJava)

    val optionsMap: java.util.Map[String, AnyRef] =
      Map[String, AnyRef]("index_mappings" -> """{ "_source": { "enabled": false } }""").asJava

    builder.options(optionsMap)

    val metadata = builder.build()
    FlintOpenSearchIndexMetadataService.serialize(metadata) should matchJson(
      testMetadataJsonWithIndexMappingSourceEnabledFalse)
  }

  "serialize" should "serialize all fields (include _source true) to JSON" in {
    val builder = new FlintMetadata.Builder
    builder.name("test_index")
    builder.kind("test_kind")
    builder.source("test_source_table")
    builder.addIndexedColumn(Map[String, AnyRef]("test_field" -> "spark_type").asJava)
    builder.schema(Map[String, AnyRef]("test_field" -> Map("type" -> "os_type").asJava).asJava)

    val optionsMap: java.util.Map[String, AnyRef] =
      Map[String, AnyRef]("index_mappings" -> """{ "_source": { "enabled": true } }""").asJava

    builder.options(optionsMap)

    val metadata = builder.build()
    FlintOpenSearchIndexMetadataService.serialize(metadata) should matchJson(
      testMetadataJsonWithIndexMappingSourceEnabledTrue)
  }

  "serialize" should "serialize all fields (include other things than _source) to JSON" in {
    val builder = new FlintMetadata.Builder
    builder.name("test_index")
    builder.kind("test_kind")
    builder.source("test_source_table")
    builder.addIndexedColumn(Map[String, AnyRef]("test_field" -> "spark_type").asJava)
    builder.schema(Map[String, AnyRef]("test_field" -> Map("type" -> "os_type").asJava).asJava)

    val optionsMap: java.util.Map[String, AnyRef] =
      Map[String, AnyRef]("index_mappings" -> """{ "boost": 1.0 }""").asJava

    builder.options(optionsMap)

    val metadata = builder.build()
    FlintOpenSearchIndexMetadataService.serialize(metadata) should matchJson(
      testMetadataJsonOtherThanSource)
  }

  "serialize" should "serialize all fields (include other things than enabled) to JSON" in {
    val builder = new FlintMetadata.Builder
    builder.name("test_index")
    builder.kind("test_kind")
    builder.source("test_source_table")
    builder.addIndexedColumn(Map[String, AnyRef]("test_field" -> "spark_type").asJava)
    builder.schema(Map[String, AnyRef]("test_field" -> Map("type" -> "os_type").asJava).asJava)

    val optionsMap: java.util.Map[String, AnyRef] = Map[String, AnyRef](
      "index_mappings" -> """{ "_source": { "includes": "meta.*" } }""").asJava

    builder.options(optionsMap)

    val metadata = builder.build()
    FlintOpenSearchIndexMetadataService.serialize(metadata) should matchJson(
      testMetadataJsonOtherThanEnabled)
  }

  "serialize" should "serialize all fields (include _source and schema merging) to JSON" in {
    val builder = new FlintMetadata.Builder
    builder.name("test_index")
    builder.kind("test_kind")
    builder.source("test_source_table")
    builder.addIndexedColumn(Map[String, AnyRef]("test_field" -> "spark_type").asJava)
    val schema = Map[String, AnyRef](
      "test_field" -> Map("type" -> "os_type", "index" -> false).asJava).asJava
    builder.schema(schema)

    val optionsMap: java.util.Map[String, AnyRef] = Map[String, AnyRef](
      "index_mappings" -> """{ "_source": { "enabled": false }, "properties": { "test_field": {"index": false} } }""").asJava

    builder.options(optionsMap)

    val metadata = builder.build()
    FlintOpenSearchIndexMetadataService.serialize(metadata) should matchJson(
      testMetadataJsonWithSourceEnabledFalseAndSchemaMerging)
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
