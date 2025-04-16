/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.udt

import inet.ipaddr.{AddressStringException, IPAddressString, IPAddressStringParameters}

import org.apache.spark.sql.SparkSession

/**
 * UDFs related to IPAddress UDT
 */
object IPFunctions {

  val valOptions = new IPAddressStringParameters.Builder()
    .allowEmpty(false)
    .setEmptyAsLoopback(false)
    .allow_inet_aton(false)
    .allowSingleSegment(false)
    .toParams;

  /**
   * TODO: come up with common implementation for both SQL and PPL {@link SerializableUdf}
   */
  val cidrMatch = (ipAddress: IPAddress, cidrBlock: String) => {
    val parsedIpAddress = new IPAddressString(ipAddress.address, valOptions)
    try {
      parsedIpAddress.validate()
    } catch {
      case e: AddressStringException =>
        throw new RuntimeException(
          s"The given ipAddress '$ipAddress' is invalid. It must be a valid IPv4 or IPv6 address. Error details: ${e.getMessage}")
    }

    val parsedCidrBlock = new IPAddressString(cidrBlock, valOptions)
    try {
      parsedCidrBlock.validate()
    } catch {
      case e: AddressStringException =>
        throw new RuntimeException(
          s"The given cidrBlock '$cidrBlock' is invalid. It must be a valid CIDR or netmask. Error details: ${e.getMessage}")
    }

    parsedCidrBlock.contains(parsedIpAddress)
  }

  def registerFunctions(spark: SparkSession): Unit = {
    spark.udf.register("cidrmatch", cidrMatch)
  }
}
