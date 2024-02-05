/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * This file contains code from the Apache Spark project (original license below).
 * It contains modifications, which are licensed as above:
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.flint.core.field.bloomfilter.classic;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Bit array.
 */
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

  /**
   * @return array length in bits
   */
  long bitSize() {
    return (long) data.length * Long.SIZE;
  }

  /**
   * @param index bit index
   * @return whether bits at the given index is set
   */
  boolean get(long index) {
    return (data[(int) (index >>> 6)] & (1L << index)) != 0;
  }

  /**
   * Set bits at the given index.
   *
   * @param index bit index
   * @return bit changed or not
   */
  boolean set(long index) {
    if (!get(index)) {
      data[(int) (index >>> 6)] |= (1L << index);
      bitCount++;
      return true;
    }
    return false;
  }

  /**
   * Put another array in this bit array.
   *
   * @param array other bit array
   */
  void putAll(BitArray array) {
    assert data.length == array.data.length : "BitArrays must be of equal length when merging";
    long bitCount = 0;
    for (int i = 0; i < data.length; i++) {
      data[i] |= array.data[i];
      bitCount += Long.bitCount(data[i]);
    }
    this.bitCount = bitCount;
  }

  /**
   * Serialize and write out this bit array to the given output stream.
   *
   * @param out output stream
   */
  void writeTo(DataOutputStream out) throws IOException {
    out.writeInt(data.length);
    for (long datum : data) {
      out.writeLong(datum);
    }
  }

  /**
   * Deserialize and read bit array from the given input stream.
   *
   * @param in input stream
   * @return bit array
   */
  static BitArray readFrom(DataInputStream in) throws IOException {
    int numWords = in.readInt();
    long[] data = new long[numWords];
    for (int i = 0; i < numWords; i++) {
      data[i] = in.readLong();
    }
    return new BitArray(data);
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

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (!(other instanceof BitArray)) return false;
    BitArray that = (BitArray) other;
    return Arrays.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(data);
  }
}
