/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.table

import java.util.Optional

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.opensearch.client.opensearch.core.pit.{CreatePitRequest, CreatePitResponse}
import org.opensearch.client.opensearch.indices.stats.IndicesStats
import org.opensearch.flint.core.{FlintOptions, IRestHighLevelClient}
import org.opensearch.flint.core.storage.OpenSearchClientUtils
import org.opensearch.flint.table.{JsonSchema, MetaData, OpenSearchIndexSnapshotTable, OpenSearchIndexTable}
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
  private val pitResponse = mock[CreatePitResponse]

  before {
    clientUtils
      .when(() => OpenSearchClientUtils.createClient(any(classOf[FlintOptions])))
      .thenReturn(openSearchClient)
    when(openSearchClient.createPit(any[CreatePitRequest]))
      .thenReturn(pitResponse)
    when(pitResponse.pitId()).thenReturn("")
  }

  def mockTable(
      scrollSize: Option[Int],
      docCount: Long,
      storeSizeInBytes: Long): OpenSearchIndexTable = {
    val metaData = mock[MetaData]
    val options = mock[FlintOptions]
    val mockIndexStats = mock[IndicesStats](RETURNS_DEEP_STUBS)

    when(metaData.name).thenReturn("test-index")
    scrollSize match {
      case Some(size) =>
        when(options.getScrollSize).thenReturn(Optional.of(Integer.valueOf(size)))
      case None => when(options.getScrollSize).thenReturn(Optional.empty[Integer]())
    }
    when(mockIndexStats.total().docs().count()).thenReturn(docCount)
    when(mockIndexStats.total().store().sizeInBytes).thenReturn(storeSizeInBytes)

    new OpenSearchIndexTable(metaData, options) {
      override lazy val indexStats: IndicesStats = mockIndexStats
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

  it should "create a PIT snapshot correctly" in {
    val metaData = mock[MetaData]
    val options = mock[FlintOptions]
    val pitResponse = mock[CreatePitResponse]

    when(metaData.name).thenReturn("test-index")
    when(openSearchClient.createPit(any[CreatePitRequest]))
      .thenReturn(pitResponse)
    when(pitResponse.pitId()).thenReturn("")
    when(options.getScrollDuration).thenReturn(5)

    val table = new OpenSearchIndexTable(metaData, options)

    val snapshotTable = table.snapshot()
    snapshotTable shouldBe a[OpenSearchIndexSnapshotTable]
    verify(openSearchClient).createPit(any[CreatePitRequest])
  }

  it should "throw UnsupportedOperationException for slice method" in {
    val metaData = mock[MetaData]
    val options = mock[FlintOptions]

    val table = new OpenSearchIndexTable(metaData, options)

    an[UnsupportedOperationException] should be thrownBy table.slice()
  }

  it should "throw UnsupportedOperationException for createReader method" in {
    val metaData = mock[MetaData]
    val options = mock[FlintOptions]

    val table = new OpenSearchIndexTable(metaData, options)

    an[UnsupportedOperationException] should be thrownBy table.createReader("query")
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
}
