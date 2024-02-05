/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.field.bloomfilter.classic;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.opensearch.flint.core.field.bloomfilter.BloomFilter;

/**
 * Classic bloom filter implementation.
 */
public class ClassicBloomFilter implements BloomFilter {

  private final BitArray bits;

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

  public static BloomFilter readFrom(InputStream in) throws IOException {
    DataInputStream dis = new DataInputStream(in);

    int version = dis.readInt();
    if (version != Version.V1.getVersionNumber()) {
      throw new IOException("Unexpected Bloom filter version number (" + version + ")");
    }

    int numHashFunctions = dis.readInt();
    BitArray bits = BitArray.readFrom(dis);
    return new ClassicBloomFilter(bits, numHashFunctions);
  }

  private static int optimalNumOfHashFunctions(long n, long m) {
    // (m / n) * log(2), but avoid truncation due to division!
    return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
  }

  private static long optimalNumOfBits(long n, double p) {
    return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
  }
}
