/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.table

/**
 * Represents information about a shard in OpenSearch.
 *
 * @param indexName
 *   The name of the index.
 * @param id
 *   The ID of the shard.
 */
case class ShardInfo(indexName: String, id: Int)
