/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.field.bloomfilter.classic;

class BitArray {
  private final long[] data;
  private long bitCount;

  BitArray(long numBits) {
    this(new long[numWords(numBits)]);
  }

  BitArray(long[] data) {
    this.data = data;
    long bitCount = 0;
    for (long word : data) {
      bitCount += Long.bitCount(word);
    }
    this.bitCount = bitCount;
  }

  long bitSize() {
    return (long) data.length * Long.SIZE;
  }

  boolean get(long index) {
    return (data[(int) (index >>> 6)] & (1L << index)) != 0;
  }

  boolean set(long index) {
    if (!get(index)) {
      data[(int) (index >>> 6)] |= (1L << index);
      bitCount++;
      return true;
    }
    return false;
  }

  void putAll(BitArray array) {
    assert data.length == array.data.length : "BitArrays must be of equal length when merging";
    long bitCount = 0;
    for (int i = 0; i < data.length; i++) {
      data[i] |= array.data[i];
      bitCount += Long.bitCount(data[i]);
    }
    this.bitCount = bitCount;
  }

  private static int numWords(long numBits) {
    if (numBits <= 0) {
      throw new IllegalArgumentException("numBits must be positive, but got " + numBits);
    }
    long numWords = (long) Math.ceil(numBits / 64.0);
    if (numWords > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Can't allocate enough space for " + numBits + " bits");
    }
    return (int) numWords;
  }
}
