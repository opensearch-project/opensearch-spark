/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

import org.opensearch.flint.core.field.bloomfilter.BloomFilterFactory
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.IndexColumnExtractor
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{BLOOM_FILTER, SkippingKind}

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, Literal}
import org.apache.spark.sql.functions.{col, xxhash64}

/**
 * Skipping strategy based on approximate data structure bloom filter.
 */
case class BloomFilterSkippingStrategy(
    override val kind: SkippingKind = BLOOM_FILTER,
    override val columnName: String,
    override val columnType: String,
    params: Map[String, String] = Map.empty)
    extends FlintSparkSkippingStrategy {

  private val bloomFilterFactory: BloomFilterFactory = BloomFilterFactory.of(params.asJava)

  override val parameters: Map[String, String] = bloomFilterFactory.getParameters.asScala.toMap

  override def outputSchema(): Map[String, String] = Map(columnName -> "binary")

  override def getAggregators: Seq[Expression] = {
    Seq(
      new BloomFilterAgg(xxhash64(col(columnName)).expr, bloomFilterFactory)
        .toAggregateExpression()
    ) // TODO: use xxhash64() for now
  }

  override def rewritePredicate(predicate: Expression): Option[Expression] = {
    val IndexColumn = IndexColumnExtractor(columnName)
    predicate match {
      case EqualTo(IndexColumn(indexCol), value: Literal) =>
        Some(BloomFilterMightContain(indexCol.expr, xxhash64(new Column(value)).expr))
      case _ => None
    }
  }
}
