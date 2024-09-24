/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import scala.collection.JavaConverters._

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.core.storage.{FlintOpenSearchClient, FlintOpenSearchIndexMetadataService}
import org.opensearch.flint.core.table.OpenSearchCluster
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.flint.config.FlintSparkConf.REFRESH_POLICY

class FlintOpenSearchClientSuite extends AnyFlatSpec with OpenSearchSuite with Matchers {

  /** Lazy initialize after container started. */
  lazy val options = new FlintOptions(openSearchOptions.asJava)
  lazy val flintClient = new FlintOpenSearchClient(options)
  lazy val flintIndexMetadataService = new FlintOpenSearchIndexMetadataService(options)

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

    val metadata = FlintOpenSearchIndexMetadataService.deserialize(content)
    flintClient.createIndex(indexName, metadata)
    flintIndexMetadataService.updateIndexMetadata(indexName, metadata)

    flintClient.exists(indexName) shouldBe true
    flintIndexMetadataService.getIndexMetadata(indexName).kind shouldBe "test_kind"
  }

  it should "create index with settings" in {
    val indexName = "flint_test_with_settings"
    val indexSettings = "{\"number_of_shards\": 3,\"number_of_replicas\": 2}"
    val metadata = FlintOpenSearchIndexMetadataService.deserialize("{}", indexSettings)

    flintClient.createIndex(indexName, metadata)
    flintClient.exists(indexName) shouldBe true

    // OS uses full setting name ("index" prefix) and store as string
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    val settings = parse(flintIndexMetadataService.getIndexMetadata(indexName).indexSettings.get)
    (settings \ "index.number_of_shards").extract[String] shouldBe "3"
    (settings \ "index.number_of_replicas").extract[String] shouldBe "2"
  }

  it should "get all index names with the given index name pattern" in {
    val metadata = FlintOpenSearchIndexMetadataService.deserialize(
      """{"properties": {"test": { "type": "integer" } } }""")
    flintClient.createIndex("flint_test_1_index", metadata)
    flintClient.createIndex("flint_test_2_index", metadata)

    val indexNames = flintClient.getIndexNames("flint_*_index")
    indexNames should have size 2
    indexNames should contain allOf ("flint_test_1_index", "flint_test_2_index")
  }

  it should "convert index name to all lowercase" in {
    val indexName = "flint_ELB_logs_index"
    flintClient.createIndex(
      indexName,
      FlintOpenSearchIndexMetadataService.deserialize(
        """{"properties": {"test": { "type": "integer" } } }"""))

    flintClient.exists(indexName) shouldBe true
    flintIndexMetadataService.getIndexMetadata(indexName) should not be null
    flintIndexMetadataService.getAllIndexMetadata("flint_ELB_*") should not be empty

    // Read write test
    val writer = flintClient.createWriter(indexName)
    writer.write("""{"create":{}}""")
    writer.write("\n")
    writer.write("""{"test":1}""")
    writer.write("\n")
    writer.flush()
    writer.close()
    createTable(indexName, options).createReader("").hasNext shouldBe true

    flintClient.deleteIndex(indexName)
    flintClient.exists(indexName) shouldBe false
  }

  it should "percent-encode invalid index name characters" in {
    val indexName = "test :\"+/\\|?#><"
    flintClient.createIndex(
      indexName,
      FlintOpenSearchIndexMetadataService.deserialize(
        """{"properties": {"test": { "type": "integer" } } }"""))

    flintClient.exists(indexName) shouldBe true
    flintIndexMetadataService.getIndexMetadata(indexName) should not be null
    flintIndexMetadataService.getAllIndexMetadata("test *") should not be empty

    // Read write test
    val writer = flintClient.createWriter(indexName)
    writer.write("""{"create":{}}""")
    writer.write("\n")
    writer.write("""{"test":1}""")
    writer.write("\n")
    writer.flush()
    writer.close()
    createTable(indexName, options).createReader("").hasNext shouldBe true

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
      val reader = createTable(indexName, options).createReader(match_all)

      reader.hasNext shouldBe true
      reader.next shouldBe """{"accountId":"123","eventName":"event","eventSource":"source"}"""
      reader.hasNext shouldBe false
      reader.close()
    }
  }

  it should "read docs from index with multiple shard successfully" in {
    val indexName = "t0001"
    val expectedCount = 5
    withIndexName(indexName) {
      multipleShardAndDocIndex(indexName, expectedCount)
      val match_all = null
      val reader = createTable(indexName, options).createReader(match_all)

      var totalCount = 0
      while (reader.hasNext) {
        reader.next()
        totalCount += 1
      }
      totalCount shouldBe expectedCount
    }
  }

  it should "read docs from shard table successfully" in {
    val indexName = "t0001"
    val expectedCount = 5
    withIndexName(indexName) {
      multipleShardAndDocIndex(indexName, expectedCount)
      val match_all = null
      val totalCount = createTable(indexName, options)
        .slice()
        .map(shardTable => {
          val reader = shardTable.createReader(match_all)
          var count = 0
          while (reader.hasNext) {
            reader.next()
            count += 1
          }
          count
        })
        .sum

      totalCount shouldBe expectedCount
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
      val reader =
        createTable(indexName, new FlintOptions(options.asJava)).createReader(match_all)
      reader.hasNext shouldBe true
      reader.next shouldBe """{"aInt":1}"""
      reader.hasNext shouldBe false
      reader.close()
    }
  }

  def createTable(indexName: String, options: FlintOptions): Table = {
    OpenSearchCluster.apply(indexName, options).asScala.head
  }
}
