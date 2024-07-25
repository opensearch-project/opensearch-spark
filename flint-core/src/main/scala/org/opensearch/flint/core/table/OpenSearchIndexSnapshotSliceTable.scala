/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.table

import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchClientUtils, OpenSearchPITSearchAfterQueryReader}
import org.opensearch.search.slice.SliceBuilder

class OpenSearchIndexSnapshotSliceTable(
    metaData: MetaData,
    option: FlintOptions,
    pit: String,
    sliceId: Int,
    maxSlice: Int)
    extends OpenSearchIndexSnapshotTable(metaData, option, pit) {

  override def snapshot(): Table = { this }

  override def slice(): Seq[Table] = {
    throw new UnsupportedOperationException("Can't slice OpenSearchIndexSnapshotSliceTable")
  }

  override def createReader(query: String): FlintReader = {
    new OpenSearchPITSearchAfterQueryReader(
      OpenSearchClientUtils.createClient(option),
      searchSourceBuilder(query).slice(new SliceBuilder(sliceId, maxSlice)))
  }
}
