/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.table

import org.opensearch.action.search.SearchRequest
import org.opensearch.flint.core.{FlintOptions, MetaData, Table}
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchClientUtils, OpenSearchSearchAfterQueryReader}
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder

/**
 * Represents an OpenSearch index shard.
 *
 * @param metaData
 *   MetaData containing information about the OpenSearch index.
 * @param option
 *   FlintOptions containing configuration options for the Flint client.
 * @param shardId
 *   Shard Id.
 */
class OpenSearchIndexShardTable(metaData: MetaData, option: FlintOptions, shardId: Int)
    extends OpenSearchIndexTable(metaData, option) {

  override def slice(): Seq[Table] = {
    throw new UnsupportedOperationException("Can't slice OpenSearchIndexShardTable")
  }

  override def createReader(query: String): FlintReader = {
    new OpenSearchSearchAfterQueryReader(
      OpenSearchClientUtils.createClient(option),
      new SearchRequest()
        .indices(name)
        .source(
          new SearchSourceBuilder()
            .query(Table.queryBuilder(query))
            .size(pageSize)
            .sort("_doc", SortOrder.ASC))
        .preference(s"_shards:$shardId"))
  }
}
