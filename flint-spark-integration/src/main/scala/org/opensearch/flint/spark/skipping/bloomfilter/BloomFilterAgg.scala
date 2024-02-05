/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.bloomfilter

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.opensearch.flint.core.field.bloomfilter.BloomFilter
import org.opensearch.flint.core.field.bloomfilter.classic.ClassicBloomFilter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.types.{BinaryType, DataType}

/**
 * An aggregate function that builds a bloom filter and serializes it to binary as the result.
 * This implementation is a customized version inspired by Spark's built-in BloomFilterAggregate.
 * Spark's implementation only accepts number of bits, uses BloomFilterImpl and cannot be extended
 * due to Scala case class restriction.
 *
 * @param child
 *   child expression that generate Long values for creating a bloom filter
 * @param expectedNumItems
 *   expected maximum unique number of items
 * @param fpp
 *   false positive probability
 */
case class BloomFilterAgg(
    child: Expression,
    expectedNumItems: Int,
    fpp: Double,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[BloomFilter] {

  def this(child: Expression, expectedNumItems: Int, fpp: Double) = {
    this(child, expectedNumItems, fpp, 0, 0)
  }

  override def nullable: Boolean = true

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = Seq(child)

  override def createAggregationBuffer(): BloomFilter = {
    new ClassicBloomFilter(expectedNumItems, fpp)
  }

  override def update(buffer: BloomFilter, inputRow: InternalRow): BloomFilter = {
    val value = child.eval(inputRow)
    if (value == null) { // Ignore null values
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
    val bloomFilter = ClassicBloomFilter.readFrom(in)
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
