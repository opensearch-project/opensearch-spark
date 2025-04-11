/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.udt

import java.net.{InetAddress, UnknownHostException}

import org.apache.spark.sql.types.SQLUserDefinedType

/**
 * A wrapper for IP address.
 * @param address
 *   IP address which could be IPv4 or IPv6.
 */
@SQLUserDefinedType(udt = classOf[IPAddressUDT])
case class IPAddress(address: String) extends Ordered[IPAddress] {

  def normalized: String = {
    try {
      InetAddress.getByName(address).getHostAddress
    } catch {
      case _: UnknownHostException => address // fallback
    }
  }

  override def compare(that: IPAddress): Int = {
    this.normalized.compare(that.normalized)
  }

  def eq(that: String): Boolean = {
    this.normalized.equals(that)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: IPAddress => this.normalized == other.normalized
      case other: String => this.normalized == IPAddress(other).normalized
      case _ => false
    }
  }

  override def hashCode(): Int = normalized.hashCode
}
