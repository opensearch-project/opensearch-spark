/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.table

import org.opensearch.common.unit.TimeValue
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.{FlintOpenSearchClient, FlintReader, OpenSearchClientUtils, OpenSearchPITSearchAfterQueryReader}
import org.opensearch.search.builder.{PointInTimeBuilder, SearchSourceBuilder}
import org.opensearch.search.sort.SortOrder

class OpenSearchIndexSnapshotTable(metaData: MetaData, option: FlintOptions, pit: String)
    extends OpenSearchIndexTable(metaData, option) {

  override def snapshot(): Table = this

  override def isSplittable(): Boolean = numberOfShards > 1

  override def slice(): Seq[Table] = {
    Range(0, numberOfShards).map(sliceId =>
      new OpenSearchIndexSnapshotSliceTable(metaData, option, pit, sliceId, numberOfShards))
  }

  override def createReader(query: String): FlintReader = {
    new OpenSearchPITSearchAfterQueryReader(
      OpenSearchClientUtils.createClient(option),
      searchSourceBuilder(query))
  }

  protected def searchSourceBuilder(query: String): SearchSourceBuilder = {
    new SearchSourceBuilder()
      .query(FlintOpenSearchClient.queryBuilder(query))
      .pointInTimeBuilder(new PointInTimeBuilder(pit).setKeepAlive(
        TimeValue.timeValueMinutes(option.getScrollDuration())))
      .size(pageSize)
      .sort("_doc", SortOrder.ASC)
  }
}
