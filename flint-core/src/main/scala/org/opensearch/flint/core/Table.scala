/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import java.io.IOException
import java.util

import com.google.common.base.Strings
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.{NamedXContentRegistry, XContentType}
import org.opensearch.common.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS
import org.opensearch.flint.core.storage.FlintReader
import org.opensearch.index.query.{AbstractQueryBuilder, MatchAllQueryBuilder, QueryBuilder}
import org.opensearch.plugins.SearchPlugin
import org.opensearch.search.SearchModule

/**
 * A OpenSearch Table.
 */
trait Table extends Serializable {

  /**
   * OpenSearch Table MetaData.
   *
   * @return
   *   {@link Table}
   */
  def metaData(): MetaData

  /**
   * Is OpenSearch Table splittable.
   *
   * @return
   *   true if splittable, otherwise false.
   */
  def isSplittable(): Boolean = false

  /**
   * Slice OpenSearch Table.
   * @return
   *   a sequence of sliced OpenSearch Table
   */
  def slice(): Seq[Table]

  /**
   * Create Flint Reader from DSL query.
   *
   * @param query
   *   OpenSearch DSL query.
   * @return
   */
  def createReader(query: String): FlintReader

  /**
   * OpenSearch Table schema
   *
   * @return
   *   {@link Schema}
   */
  def schema(): Schema
}

object Table {

  /**
   * {@link NamedXContentRegistry} from {@link SearchModule} used for construct {@link
   * QueryBuilder} from DSL query string.
   */
  val xContentRegistry = new NamedXContentRegistry(
    new SearchModule(Settings.builder.build, new util.ArrayList[SearchPlugin]).getNamedXContents)

  @throws[IOException]
  def queryBuilder(query: String): QueryBuilder = {
    if (!Strings.isNullOrEmpty(query)) {
      val parser =
        XContentType.JSON.xContent.createParser(xContentRegistry, IGNORE_DEPRECATIONS, query)
      AbstractQueryBuilder.parseInnerQueryBuilder(parser)
    } else {
      new MatchAllQueryBuilder
    }
  }
}
