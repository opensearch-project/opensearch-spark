/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.udt

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class IPAddressUDT extends UserDefinedType[IPAddress] {

  override def sqlType: DataType = StringType

  override def serialize(obj: IPAddress): UTF8String = UTF8String.fromString(obj.address)

  override def deserialize(datum: Any): IPAddress = datum match {
    case row: UTF8String => IPAddress(row.toString)
  }
  override def userClass: Class[IPAddress] = classOf[IPAddress]

  override def typeName: String = "ip"

  override def equals(o: Any): Boolean = {
    o match {
      case v: IPAddressUDT => true
      case _ => false
    }
  }

  override def hashCode(): Int = getClass.getName.hashCode()
}

case object IPAddressUDT extends IPAddressUDT
