/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilter.BloomFilterFactory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.types.{BinaryType, DataType}

/**
 * Aggregate function that build bloom filter and serialize to binary as result. Copy from Spark
 * built-in BloomFilterAggregate because it: 1) it accepts number of bits as argument instead of
 * FPP 2) it calls static method BloomFilter.create and thus cannot change to other implementation
 * 3) it is a Scala case class that cannot be extend and overridden
 *
 * @param child
 *   child expression of
 * @param bloomFilter
 * @param mutableAggBufferOffset
 * @param inputAggBufferOffset
 */
case class BloomFilterAgg(
    child: Expression,
    bloomFilterFactory: BloomFilterFactory,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[BloomFilter] {

  def this(child: Expression, bloomFilterFactory: BloomFilterFactory) = {
    this(child, bloomFilterFactory, 0, 0)
  }

  override def nullable: Boolean = true

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = Seq(child)

  override def createAggregationBuffer(): BloomFilter = bloomFilterFactory.create()

  override def update(buffer: BloomFilter, inputRow: InternalRow): BloomFilter = {
    val value = child.eval(inputRow)
    // Ignore null values.
    if (value == null) {
      return buffer
    }
    buffer.put(value.asInstanceOf[Long])
    buffer
  }

  override def merge(buffer: BloomFilter, input: BloomFilter): BloomFilter = {
    buffer.merge(input)
    buffer
  }

  override def eval(buffer: BloomFilter): Any = {
    if (buffer.bitSize() == 0) {
      // There's no set bit in the Bloom filter and hence no not-null value is processed.
      return null
    }
    serialize(buffer)
  }

  override def serialize(buffer: BloomFilter): Array[Byte] = {
    // BloomFilterImpl.writeTo() writes 2 integers (version number and num hash functions), hence
    // the +8
    val size = (buffer.bitSize() / 8) + 8
    require(size <= Integer.MAX_VALUE, s"actual number of bits is too large $size")
    val out = new ByteArrayOutputStream(size.intValue())
    buffer.writeTo(out)
    out.close()
    out.toByteArray
  }

  override def deserialize(bytes: Array[Byte]): BloomFilter = {
    val in = new ByteArrayInputStream(bytes)
    val bloomFilter = bloomFilterFactory.deserialize(in)
    in.close()
    bloomFilter
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(child = newChildren.head)

  override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newOffset)
}
