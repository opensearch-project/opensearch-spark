/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.udt

import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, UserDefinedType}

class GeoPointUDT extends UserDefinedType[GeoPoint] {

  override def sqlType: DataType = ArrayType(DoubleType, containsNull = false)

  override def serialize(obj: GeoPoint): Array[Double] = Array(obj.lat, obj.lon)

  override def deserialize(datum: Any): GeoPoint = datum match {
    case arr: ArrayData if arr.numElements() == 2 =>
      val lat = arr.getDouble(0)
      val lon = arr.getDouble(1)
      GeoPoint(lat, lon)
    case _ =>
      throw new IllegalArgumentException(s"Cannot deserialize $datum to GeoPoint")
  }

  override def userClass: Class[GeoPoint] = classOf[GeoPoint]

  override def typeName: String = "geo_point"

  override def equals(o: Any): Boolean = {
    o match {
      case _: GeoPointUDT => true
      case _ => false
    }
  }

  override def hashCode(): Int = getClass.getName.hashCode()
}

case object GeoPointUDT extends GeoPointUDT
