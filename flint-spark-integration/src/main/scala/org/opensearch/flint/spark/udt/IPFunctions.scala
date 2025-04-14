/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.udt

import org.apache.spark.sql.SparkSession

/**
 * UDFs related to IPAddress UDT
 */
object IPFunctions {
  // Convert String to IPAddress
  val stringToIp = (ip: String) => { IPAddress(ip) }

  // Convert IPAddress to String
  val ipToString = (ip: IPAddress) => { ip.address }

  // Match IPAddress with String. This will match different notation.
  val ipEqual = (ip1: IPAddress, ip2: String) => { ip1.compare(IPAddress(ip2)) == 0 }

  def registerFunctions(spark: SparkSession): Unit = {
    spark.udf.register("string_to_ip", stringToIp)
    spark.udf.register("ip_to_string", ipToString)
    spark.udf.register("ip_equal", ipEqual)
  }
}
