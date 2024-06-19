/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import scala.collection.JavaConverters._

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.mockito.Mockito.when
import org.opensearch.client.json.jackson.JacksonJsonpMapper
import org.opensearch.client.opensearch.OpenSearchClient
import org.opensearch.client.transport.rest_client.RestClientTransport
import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.core.storage.{FlintOpenSearchClient, OpenSearchScrollReader}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.sql.flint.config.FlintSparkConf.{DATA_SOURCE_NAME, REFRESH_POLICY, SCROLL_DURATION, SCROLL_SIZE}

class FlintOpenSearchClientSuite extends AnyFlatSpec with OpenSearchSuite with Matchers {

  /** Lazy initialize after container started. */
  lazy val flintClient = new FlintOpenSearchClient(new FlintOptions(openSearchOptions.asJava))

  behavior of "Flint OpenSearch client"

  it should "create index successfully" in {
    val indexName = "test"
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

    val metadata = mock[FlintMetadata]
    when(metadata.getContent).thenReturn(content)
    when(metadata.indexSettings).thenReturn(None)
    flintClient.createIndex(indexName, metadata)

    flintClient.exists(indexName) shouldBe true
    flintClient.getIndexMetadata(indexName).kind shouldBe "test_kind"
  }

  it should "create index with settings" in {
    val indexName = "flint_test_with_settings"
    val indexSettings = "{\"number_of_shards\": 3,\"number_of_replicas\": 2}"
    val metadata = mock[FlintMetadata]
    when(metadata.getContent).thenReturn("{}")
    when(metadata.indexSettings).thenReturn(Some(indexSettings))

    flintClient.createIndex(indexName, metadata)
    flintClient.exists(indexName) shouldBe true

    // OS uses full setting name ("index" prefix) and store as string
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    val settings = parse(flintClient.getIndexMetadata(indexName).indexSettings.get)
    (settings \ "index.number_of_shards").extract[String] shouldBe "3"
    (settings \ "index.number_of_replicas").extract[String] shouldBe "2"
  }

  it should "update index successfully" in {
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

    val metadata = mock[FlintMetadata]
    when(metadata.getContent).thenReturn(content)
    when(metadata.indexSettings).thenReturn(None)
    flintClient.createIndex(indexName, metadata)

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

    val newMetadata = mock[FlintMetadata]
    when(newMetadata.getContent).thenReturn(newContent)
    when(newMetadata.indexSettings).thenReturn(None)
    flintClient.updateIndex(indexName, newMetadata)

    flintClient.exists(indexName) shouldBe true
    flintClient.getIndexMetadata(indexName).kind shouldBe "test_kind"
    flintClient.getIndexMetadata(indexName).name shouldBe "test_name"
  }

  it should "get all index metadata with the given index name pattern" in {
    val metadata = mock[FlintMetadata]
    when(metadata.getContent).thenReturn("{}")
    when(metadata.indexSettings).thenReturn(None)
    flintClient.createIndex("flint_test_1_index", metadata)
    flintClient.createIndex("flint_test_2_index", metadata)

    val allMetadata = flintClient.getAllIndexMetadata("flint_*_index")
    allMetadata should have size 2
    allMetadata.values.forEach(metadata => metadata.getContent should not be empty)
    allMetadata.values.forEach(metadata => metadata.indexSettings should not be empty)
  }

  it should "convert index name to all lowercase" in {
    val indexName = "flint_ELB_logs_index"
    flintClient.createIndex(
      indexName,
      FlintMetadata("""{"properties": {"test": { "type": "integer" } } }"""))

    flintClient.exists(indexName) shouldBe true
    flintClient.getIndexMetadata(indexName) should not be null
    flintClient.getAllIndexMetadata("flint_ELB_*") should not be empty

    // Read write test
    val writer = flintClient.createWriter(indexName)
    writer.write("""{"create":{}}""")
    writer.write("\n")
    writer.write("""{"test":1}""")
    writer.write("\n")
    writer.flush()
    writer.close()
    flintClient.createReader(indexName, "").hasNext shouldBe true

    flintClient.deleteIndex(indexName)
    flintClient.exists(indexName) shouldBe false
  }

  it should "percent-encode invalid index name characters" in {
    val indexName = "test ,:\"+/\\|?#><"
    flintClient.createIndex(
      indexName,
      FlintMetadata("""{"properties": {"test": { "type": "integer" } } }"""))

    flintClient.exists(indexName) shouldBe true
    flintClient.getIndexMetadata(indexName) should not be null
    flintClient.getAllIndexMetadata("test *") should not be empty

    // Read write test
    val writer = flintClient.createWriter(indexName)
    writer.write("""{"create":{}}""")
    writer.write("\n")
    writer.write("""{"test":1}""")
    writer.write("\n")
    writer.flush()
    writer.close()
    flintClient.createReader(indexName, "").hasNext shouldBe true

    flintClient.deleteIndex(indexName)
    flintClient.exists(indexName) shouldBe false
  }

  it should "return false if index not exist" in {
    flintClient.exists("non-exist-index") shouldBe false
  }

  it should "read docs from index as string successfully " in {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      val match_all = null
      val reader = flintClient.createReader(indexName, match_all)

      reader.hasNext shouldBe true
      reader.next shouldBe """{"accountId":"123","eventName":"event","eventSource":"source"}"""
      reader.hasNext shouldBe false
      reader.close()
    }
  }

  it should "write docs to index successfully " in {
    val indexName = "t0001"
    withIndexName(indexName) {
      val mappings =
        """{
          |  "properties": {
          |    "aInt": {
          |      "type": "integer"
          |    }
          |  }
          |}""".stripMargin

      val options = openSearchOptions + (s"${REFRESH_POLICY.optionKey}" -> "wait_for")
      val flintClient = new FlintOpenSearchClient(new FlintOptions(options.asJava))
      index(indexName, oneNodeSetting, mappings, Seq.empty)
      val writer = flintClient.createWriter(indexName)
      writer.write("""{"create":{}}""")
      writer.write("\n")
      writer.write("""{"aInt":1}""")
      writer.write("\n")
      writer.flush()
      writer.close()

      val match_all = null
      val reader = flintClient.createReader(indexName, match_all)
      reader.hasNext shouldBe true
      reader.next shouldBe """{"aInt":1}"""
      reader.hasNext shouldBe false
      reader.close()
    }
  }

  it should "scroll context close properly after read" in {
    val indexName = "t0001"
    withIndexName(indexName) {
      simpleIndex(indexName)
      val match_all = null
      val reader = flintClient.createReader(indexName, match_all)

      reader.hasNext shouldBe true
      reader.next shouldBe """{"accountId":"123","eventName":"event","eventSource":"source"}"""
      reader.hasNext shouldBe false
      reader.close()

      reader.asInstanceOf[OpenSearchScrollReader].getScrollId shouldBe null
      scrollShouldClosed()
    }
  }

  it should "no item return after scroll timeout" in {
    val indexName = "t0001"
    withIndexName(indexName) {
      multipleDocIndex(indexName, 2)

      val options =
        openSearchOptions + (s"${SCROLL_DURATION.optionKey}" -> "1", s"${SCROLL_SIZE.optionKey}" -> "1")
      val flintClient = new FlintOpenSearchClient(new FlintOptions(options.asJava))
      val match_all = null
      val reader = flintClient.createReader(indexName, match_all)

      reader.hasNext shouldBe true
      reader.next
      // scroll context expired after 1 minutes
      Thread.sleep(60 * 1000 * 2)
      reader.hasNext shouldBe false
      reader.close()

      reader.asInstanceOf[OpenSearchScrollReader].getScrollId shouldBe null
      scrollShouldClosed()
    }
  }

  def scrollShouldClosed(): Unit = {
    val transport =
      new RestClientTransport(openSearchClient.getLowLevelClient, new JacksonJsonpMapper)
    val client = new OpenSearchClient(transport)

    val response = client.nodes().stats()
    response.nodes().size() should be > 0
    response.nodes().forEach((_, stats) => stats.indices().search().scrollCurrent() shouldBe 0)
  }
}
