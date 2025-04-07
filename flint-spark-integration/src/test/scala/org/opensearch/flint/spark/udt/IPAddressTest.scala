/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.udt

import org.apache.spark.SparkFunSuite

class IPAddressTest extends SparkFunSuite {
  test("IPAddress normalization") {
    val ip1 = IPAddress("192.168.0.1")
    val ip2 = IPAddress("192.168.0.1")
    assert(ip1 == ip2)
  }

  test("IPAddress comparison") {
    val ip1 = IPAddress("192.168.0.1")
    val ip2 = IPAddress("192.168.0.2")
    assert(ip1.compare(ip2) < 0)
  }

  test("IPv4 and IPv6 comparison") {
    val ipv4 = IPAddress("192.168.0.1")
    val ipv6 = IPAddress("::ffff:c0a8:1")
    assert(ipv4 == ipv6)
  }

  test("Different IPv6 addresses comparison") {
    val ipv6_1 = IPAddress("::1")
    val ipv6_2 = IPAddress("::2")
    assert(ipv6_1.compare(ipv6_2) < 0)
  }

  test("Different IPv6 addresses comparison") {
    val ipv6_1 = IPAddress("::1")
    val ipv6_2 = IPAddress("::1")
    assert(ipv6_1 == ipv6_2)
  }
}
