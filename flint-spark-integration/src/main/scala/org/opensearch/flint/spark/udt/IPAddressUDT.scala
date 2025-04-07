/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.udt

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

// Implement IpAddressUDT with StructType as sqlType
class IPAddressUDT extends UserDefinedType[IPAddress] {

  // Use StructType to avoid implicit equality check with String type
  override def sqlType: DataType = StructType(
    Seq(StructField("address", StringType, nullable = false)))

  override def serialize(obj: IPAddress): InternalRow = {
    val row = new GenericInternalRow(Array[Any](UTF8String.fromString(obj.address)))
    row
  }

  override def deserialize(datum: Any): IPAddress = datum match {
    case row: InternalRow => IPAddress(row.getString(0))
  }
  override def userClass: Class[IPAddress] = classOf[IPAddress]

  override def typeName: String = "ipaddress"

  override def equals(o: Any): Boolean = {
    o match {
      case v: IPAddressUDT => true
      case _ => false
    }
  }

  override def hashCode(): Int = getClass.getName.hashCode()
}

case object IPAddressUDT extends IPAddressUDT
