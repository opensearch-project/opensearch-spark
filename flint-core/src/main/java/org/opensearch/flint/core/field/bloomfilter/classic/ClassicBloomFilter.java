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
import java.io.InputStream;
import java.io.OutputStream;
import org.opensearch.flint.core.field.bloomfilter.BloomFilter;

/**
 * Classic bloom filter implementation inspired by [[org.apache.spark.util.sketch.BloomFilterImpl]]
 * but only keep minimal functionality. Bloom filter is serialized in the following format:
 * <p>
 * 1) Version number, always 1 (32 bit)
 * 2) Number of hash functions (32 bit)
 * 3) Total number of words of the underlying bit array (32 bit)
 * 4) The words/longs (numWords * 64 bit)
 */
public class ClassicBloomFilter implements BloomFilter {

  /**
   * Bit array
   */
  private final BitArray bits;

  /**
   * Number of hash function
   */
  private final int numHashFunctions;

  public ClassicBloomFilter(int expectedNumItems, double fpp) {
    long numBits = optimalNumOfBits(expectedNumItems, fpp);
    this.bits = new BitArray(numBits);
    this.numHashFunctions = optimalNumOfHashFunctions(expectedNumItems, numBits);
  }

  ClassicBloomFilter(BitArray bits, int numHashFunctions) {
    this.bits = bits;
    this.numHashFunctions = numHashFunctions;
  }

  @Override
  public long bitSize() {
    return bits.bitSize();
  }

  @Override
  public boolean put(long item) {
    int h1 = Murmur3_x86_32.hashLong(item, 0);
    int h2 = Murmur3_x86_32.hashLong(item, h1);

    long bitSize = bits.bitSize();
    boolean bitsChanged = false;
    for (int i = 1; i <= numHashFunctions; i++) {
      int combinedHash = h1 + (i * h2);
      // Flip all the bits if it's negative (guaranteed positive number)
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      bitsChanged |= bits.set(combinedHash % bitSize);
    }
    return bitsChanged;
  }

  @Override
  public BloomFilter merge(BloomFilter other) {
    if (!(other instanceof ClassicBloomFilter)) {
      throw new IllegalStateException("Cannot merge incompatible bloom filter of class"
          + other.getClass().getName());
    }
    this.bits.putAll(((ClassicBloomFilter) other).bits);
    return this;
  }

  @Override
  public boolean mightContain(long item) {
    int h1 = Murmur3_x86_32.hashLong(item, 0);
    int h2 = Murmur3_x86_32.hashLong(item, h1);

    long bitSize = bits.bitSize();
    for (int i = 1; i <= numHashFunctions; i++) {
      int combinedHash = h1 + (i * h2);
      // Flip all the bits if it's negative (guaranteed positive number)
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      if (!bits.get(combinedHash % bitSize)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    DataOutputStream dos = new DataOutputStream(out);

    dos.writeInt(Version.V1.getVersionNumber());
    dos.writeInt(numHashFunctions);
    bits.writeTo(dos);
  }

  /**
   * Deserialize and read bloom filter from an input stream.
   *
   * @param in input stream
   * @return bloom filter
   */
  public static BloomFilter readFrom(InputStream in) {
    try {
      DataInputStream dis = new DataInputStream(in);

      // Check version compatibility
      int version = dis.readInt();
      if (version != Version.V1.getVersionNumber()) {
        throw new IllegalStateException("Unexpected Bloom filter version number (" + version + ")");
      }

      // Read bloom filter content
      int numHashFunctions = dis.readInt();
      BitArray bits = BitArray.readFrom(dis);
      return new ClassicBloomFilter(bits, numHashFunctions);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static int optimalNumOfHashFunctions(long n, long m) {
    // (m / n) * log(2), but avoid truncation due to division!
    return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
  }

  private static long optimalNumOfBits(long n, double p) {
    return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof ClassicBloomFilter)) {
      return false;
    }
    ClassicBloomFilter that = (ClassicBloomFilter) other;
    return this.numHashFunctions == that.numHashFunctions && this.bits.equals(that.bits);
  }

  @Override
  public int hashCode() {
    return bits.hashCode() * 31 + numHashFunctions;
  }
}
