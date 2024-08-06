/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.table

import java.util.Optional

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.opensearch.client.opensearch.indices.{IndicesStatsRequest, IndicesStatsResponse}
import org.opensearch.flint.core.{FlintOptions, IRestHighLevelClient, JsonSchema, MetaData}
import org.opensearch.flint.core.storage.{OpenSearchClientUtils, OpenSearchSearchAfterQueryReader}
import org.opensearch.search.builder.SearchSourceBuilder
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class OpenSearchIndexTableSpec
    extends AnyFlatSpec
    with BeforeAndAfter
    with Matchers
    with MockitoSugar {

  private val clientUtils = mockStatic(classOf[OpenSearchClientUtils])
  private val openSearchClient = mock[IRestHighLevelClient](RETURNS_DEEP_STUBS)

  before {
    clientUtils
      .when(() => OpenSearchClientUtils.createClient(any(classOf[FlintOptions])))
      .thenReturn(openSearchClient)
  }

  def mockTable(
      scrollSize: Option[Int],
      docCount: Long,
      storeSizeInBytes: Long,
      supportShard: Boolean = true,
      numberOfShards: Int = 1): OpenSearchIndexTable = {
    val metaData = mock[MetaData]
    val options = mock[FlintOptions]
    val mockIndicesStatsResp = mock[IndicesStatsResponse](RETURNS_DEEP_STUBS)

    when(metaData.name).thenReturn("test-index")
    when(metaData.setting).thenReturn(s"""{"index.number_of_shards":"$numberOfShards"}""")
    scrollSize match {
      case Some(size) =>
        when(options.getScrollSize).thenReturn(Optional.of(Integer.valueOf(size)))
      case None => when(options.getScrollSize).thenReturn(Optional.empty[Integer]())
    }
    when(options.supportShard()).thenReturn(supportShard)

    when(openSearchClient.stats(any[IndicesStatsRequest])).thenReturn(mockIndicesStatsResp)
    when(mockIndicesStatsResp.indices().get(any[String]).primaries().docs().count())
      .thenReturn(docCount)
    when(mockIndicesStatsResp.indices().get(any[String]).primaries().store().sizeInBytes)
      .thenReturn(storeSizeInBytes)

    new OpenSearchIndexTable(metaData, options) {
      override lazy val maxResultWindow: Int = 10000
    }
  }

  "OpenSearchIndexTable" should "return the correct pageSize when scroll size is present" in {
    val table = mockTable(Some(100), 1000L, 10000000L)
    table.pageSize shouldBe 100
  }

  it should "return the maxResultWindow when getScrollSize is not configured and no documents are present" in {
    val table = mockTable(None, 0L, 0L)
    table.pageSize shouldBe table.maxResultWindow
  }

  it should "return the correct pageSize when getScrollSize is not configured and docSize is less than 10MB" in {
    val docCount = 128L
    val docSize = 1 * 1024 * 1024 // 1MB
    val table = mockTable(None, docCount, docSize * docCount)
    table.pageSize shouldBe 10
  }

  it should "return 1 when getScrollSize is not configured and docSize is equal to 10MB" in {
    val docSize = 10 * 1024 * 1024 // 10MB
    val table = mockTable(None, 1L, docSize)
    table.pageSize shouldBe 1
  }

  it should "return 1 when getScrollSize is not configured and docSize is greater than 10MB" in {
    val docSize = 20 * 1024 * 1024 // 20MB
    val table = mockTable(None, 1L, docSize)
    table.pageSize shouldBe 1
  }

  it should "return the correct schema" in {
    val metaData = mock[MetaData]
    val options = mock[FlintOptions]

    when(metaData.properties).thenReturn("properties")

    val table = new OpenSearchIndexTable(metaData, options)

    table.schema() shouldBe JsonSchema("properties")
  }

  it should "return the correct metadata" in {
    val metaData = mock[MetaData]
    val options = mock[FlintOptions]

    val table = new OpenSearchIndexTable(metaData, options)

    table.metaData() shouldBe metaData
  }

  it should "return true for isSplittable when there are multiple shards" in {
    val table = mockTable(None, 1000L, 10000000L, numberOfShards = 3)
    table.isSplittable() shouldBe true
  }

  it should "return false for isSplittable when there is only one shard" in {
    val table = mockTable(None, 1000L, 10000000L, supportShard = true, numberOfShards = 1)
    table.isSplittable() shouldBe false
  }

  it should "return false for isSplittable when index does not support shard" in {
    val table = mockTable(None, 1000L, 10000000L, supportShard = false, numberOfShards = 1)
    table.isSplittable() shouldBe false
  }

  it should "return the correct number of shard tables when sliced" in {
    val numberOfShards = 3
    val table = mockTable(None, 1000L, 10000000L, numberOfShards = numberOfShards)
    val slicedTables = table.slice()
    slicedTables.size shouldBe numberOfShards
    slicedTables.foreach(_ shouldBe a[OpenSearchIndexShardTable])
  }

  it should "create a reader for the table" in {
    val query = ""
    val table = mockTable(None, 1000L, 10000000L, numberOfShards = 1)
    val reader = table.createReader(query)
    reader shouldBe a[OpenSearchSearchAfterQueryReader]

    val searchRequest = reader.asInstanceOf[OpenSearchSearchAfterQueryReader].searchRequest
    searchRequest.indices() should contain("test-index")

    val sourceBuilder = searchRequest.source().asInstanceOf[SearchSourceBuilder]
    sourceBuilder.query() should not be null
    sourceBuilder.size() shouldBe table.pageSize

    val sorts = sourceBuilder.sorts()
    sorts.size() shouldBe 2
    sorts.get(0).toString should include("{\n  \"_doc\" : {\n    \"order\" : \"asc\"\n  }\n}")
    sorts.get(1).toString should include("{\n  \"_id\" : {\n    \"order\" : \"asc\"\n  }\n}")
  }

  "OpenSearchIndexShardTable" should "create reader correctly" in {
    val query = ""
    val indexTable = mockTable(None, 1000L, 10000000L, numberOfShards = 3)
    val table = indexTable.slice().head
    val reader = table.createReader(query)
    reader shouldBe a[OpenSearchSearchAfterQueryReader]

    val searchRequest = reader.asInstanceOf[OpenSearchSearchAfterQueryReader].searchRequest
    searchRequest.indices() should contain("test-index")

    searchRequest.preference() shouldBe "_shards:0"

    val sourceBuilder = searchRequest.source()
    sourceBuilder.query() should not be null
    sourceBuilder.size() shouldBe indexTable.pageSize

    val sorts = sourceBuilder.sorts()
    sorts.size() shouldBe 1
    sorts.get(0).toString should include("{\n  \"_doc\" : {\n    \"order\" : \"asc\"\n  }\n}")
  }
}
