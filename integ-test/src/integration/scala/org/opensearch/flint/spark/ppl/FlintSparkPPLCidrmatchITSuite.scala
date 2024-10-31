/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLCidrmatchITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createIpAddressTable(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test cidrmatch for ipv4 for 192.168.1.0/24") {
    val frame = sql(s"""
                       | source = $testTable | where isV6 = false and isValid = true and cidrmatch(ipAddress, '192.168.1.0/24')
                       | """.stripMargin)

    val results = frame.collect()
    assert(results.length == 2)
  }

  test("test cidrmatch for ipv4 for 192.169.1.0/24") {
    val frame = sql(s"""
                       | source = $testTable | where isV6 = false and isValid = true and cidrmatch(ipAddress, '192.169.1.0/24')
                       | """.stripMargin)

    val results = frame.collect()
    assert(results.length == 0)
  }

  test("test cidrmatch for ipv6 for 2001:db8::/32") {
    val frame = sql(s"""
                       | source = $testTable | where isV6 = true and isValid = true and cidrmatch(ipAddress, '2001:db8::/32')
                       | """.stripMargin)

    val results = frame.collect()
    assert(results.length == 3)
  }

  test("test cidrmatch for ipv6 for 2003:db8::/32") {
    val frame = sql(s"""
                       | source = $testTable | where isV6 = true and isValid = true and cidrmatch(ipAddress, '2003:db8::/32')
                       | """.stripMargin)

    val results = frame.collect()
    assert(results.length == 0)
  }

  test("test cidrmatch for ipv6 with ipv4 cidr") {
    val frame = sql(s"""
                       | source = $testTable | where isV6 = true and isValid = true and cidrmatch(ipAddress, '192.169.1.0/24')
                       | """.stripMargin)

    assertThrows[SparkException](frame.collect())
  }

  test("test cidrmatch for invalid ipv4 addresses") {
    val frame = sql(s"""
                       | source = $testTable | where isV6 = false and isValid = false and cidrmatch(ipAddress, '192.169.1.0/24')
                       | """.stripMargin)

    assertThrows[SparkException](frame.collect())
  }

  test("test cidrmatch for invalid ipv6 addresses") {
    val frame = sql(s"""
                       | source = $testTable | where isV6 = true and isValid = false and cidrmatch(ipAddress, '2003:db8::/32')
                       | """.stripMargin)

    assertThrows[SparkException](frame.collect())
  }
}
