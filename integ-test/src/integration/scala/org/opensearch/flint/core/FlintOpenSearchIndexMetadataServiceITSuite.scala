/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import java.util

import scala.collection.JavaConverters._

import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.common.metadata.{FlintIndexMetadataService, FlintMetadata}
import org.opensearch.flint.core.metadata.FlintIndexMetadataServiceBuilder
import org.opensearch.flint.core.storage.{FlintOpenSearchClient, FlintOpenSearchIndexMetadataService}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FlintOpenSearchIndexMetadataServiceITSuite
    extends AnyFlatSpec
    with OpenSearchSuite
    with Matchers {

  /** Lazy initialize after container started. */
  lazy val options = new FlintOptions(openSearchOptions.asJava)
  lazy val flintClient = new FlintOpenSearchClient(options)
  lazy val flintIndexMetadataService = new FlintOpenSearchIndexMetadataService(options)

  behavior of "Flint index metadata service builder"

  it should "build index metadata service" in {
    val customOptions =
      openSearchOptions + (FlintOptions.CUSTOM_FLINT_INDEX_METADATA_SERVICE_CLASS -> "org.opensearch.flint.core.TestIndexMetadataService")
    val customFlintOptions = new FlintOptions(customOptions.asJava)
    val customFlintIndexMetadataService =
      FlintIndexMetadataServiceBuilder.build(customFlintOptions)
    customFlintIndexMetadataService shouldBe a[TestIndexMetadataService]
  }

  it should "fail to build index metadata service if class name doesn't exist" in {
    val options =
      openSearchOptions + (FlintOptions.CUSTOM_FLINT_INDEX_METADATA_SERVICE_CLASS -> "dummy")
    val flintOptions = new FlintOptions(options.asJava)
    the[RuntimeException] thrownBy {
      FlintIndexMetadataServiceBuilder.build(flintOptions)
    }
  }

  behavior of "Flint OpenSearch index metadata service"

  it should "get all index metadata with the given index name pattern" in {
    val metadata = FlintOpenSearchIndexMetadataService.deserialize("{}")
    flintClient.createIndex("flint_test_1_index", metadata)
    flintClient.createIndex("flint_test_2_index", metadata)

    val allMetadata = flintIndexMetadataService.getAllIndexMetadata("flint_*_index")
    allMetadata should have size 2
    allMetadata.values.forEach(metadata =>
      FlintOpenSearchIndexMetadataService.serialize(metadata) should not be empty)
    allMetadata.values.forEach(metadata => metadata.indexSettings should not be empty)
  }

  it should "update index metadata successfully" in {
    val indexName = "test_update"
    val content =
      """ {
        |   "_meta": {
        |     "kind": "test_kind"
        |   },
        |   "properties": {
        |     "age": {
        |       "type": "integer"
        |     }
        |   }
        | }
        |""".stripMargin

    val metadata = FlintOpenSearchIndexMetadataService.deserialize(content)
    flintClient.createIndex(indexName, metadata)

    flintIndexMetadataService.getIndexMetadata(indexName).kind shouldBe empty

    flintIndexMetadataService.updateIndexMetadata(indexName, metadata)

    flintIndexMetadataService.getIndexMetadata(indexName).kind shouldBe "test_kind"
    flintIndexMetadataService.getIndexMetadata(indexName).name shouldBe empty

    val newContent =
      """ {
        |   "_meta": {
        |     "kind": "test_kind",
        |     "name": "test_name"
        |   },
        |   "properties": {
        |     "age": {
        |       "type": "integer"
        |     }
        |   }
        | }
        |""".stripMargin

    val newMetadata = FlintOpenSearchIndexMetadataService.deserialize(newContent)
    flintIndexMetadataService.updateIndexMetadata(indexName, newMetadata)

    flintIndexMetadataService.getIndexMetadata(indexName).kind shouldBe "test_kind"
    flintIndexMetadataService.getIndexMetadata(indexName).name shouldBe "test_name"
  }
}

class TestIndexMetadataService extends FlintIndexMetadataService {
  override def getIndexMetadata(indexName: String): FlintMetadata = {
    null
  }

  override def getAllIndexMetadata(indexNamePattern: String*): util.Map[String, FlintMetadata] = {
    null
  }

  override def updateIndexMetadata(indexName: String, metadata: FlintMetadata): Unit = {}

  override def deleteIndexMetadata(indexName: String): Unit = {}
}
