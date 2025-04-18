/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.udt

import java.util.Locale

import scala.util.Try

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import org.locationtech.jts.geom.Point
import org.locationtech.jts.io.WKTReader
import org.opensearch.geometry.utils.Geohash

object GeoPointConverter {
  def fromJsonParser(parser: JsonParser): Option[GeoPoint] = {
    parser.currentToken() match {
      case JsonToken.VALUE_STRING =>
        parseString(parser.getText)

      case JsonToken.START_ARRAY =>
        parseArray(parser)

      case JsonToken.START_OBJECT =>
        parseObject(parser)

      case _ =>
        None
    }
  }

  private def parseString(text: String): Option[GeoPoint] = {
    val trimmed = text.trim
    if (trimmed.toUpperCase(Locale.ROOT).startsWith("POINT")) {
      parseWKT(trimmed)
    } else if (trimmed.contains(",")) {
      // Format: "lat,lon" or "lat lon"
      val parts = trimmed.split(",\\s*")
      if (parts.length == 2) {
        for {
          lat <- Try(parts(0).toDouble).toOption
          lon <- Try(parts(1).toDouble).toOption
        } yield GeoPoint(lat, lon)
      } else {
        None
      }
    } else if (isGeohash(trimmed)) {
      decodeGeohash(trimmed)
    } else {
      None
    }
  }

  private def parseArray(parser: JsonParser): Option[GeoPoint] = {
    val coords = scala.collection.mutable.ListBuffer[Double]()
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      if (parser.currentToken().isNumeric) {
        coords += parser.getDoubleValue
      }
    }
    if (coords.length == 2) {
      // Assuming format: [lon, lat]
      Some(GeoPoint(coords(1), coords(0)))
    } else {
      None
    }
  }

  private def parseObject(parser: JsonParser): Option[GeoPoint] = {
    var latOpt: Option[Double] = None
    var lonOpt: Option[Double] = None
    var typeOpt: Option[String] = None
    var coordinatesOpt: Option[(Double, Double)] = None

    while (parser.nextToken() != JsonToken.END_OBJECT) {
      val fieldName = parser.getCurrentName
      parser.nextToken()
      fieldName match {
        case "lat" => latOpt = Try(parser.getDoubleValue).toOption
        case "lon" => lonOpt = Try(parser.getDoubleValue).toOption
        case "type" => typeOpt = Some(parser.getText)
        case "coordinates" =>
          val coords = parseCoordinatesArray(parser)
          coordinatesOpt = coords
        case _ => parser.skipChildren()
      }
    }

    (latOpt, lonOpt) match {
      case (Some(lat), Some(lon)) => Some(GeoPoint(lat, lon))
      case _ =>
        (typeOpt, coordinatesOpt) match {
          case (Some("Point"), Some((lon, lat))) => Some(GeoPoint(lat, lon))
          case _ => None
        }
    }
  }

  private def parseCoordinatesArray(parser: JsonParser): Option[(Double, Double)] = {
    if (parser.currentToken() != JsonToken.START_ARRAY) return None
    val coords = scala.collection.mutable.ListBuffer[Double]()
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      if (parser.currentToken().isNumeric) {
        coords += parser.getDoubleValue
      }
    }
    if (coords.length >= 2) {
      Some((coords(0), coords(1)))
    } else {
      None
    }
  }

  private def isGeohash(text: String): Boolean = {
    val geohashPattern = "^[0-9b-hj-np-z]+$".r
    text.toLowerCase(Locale.ROOT) match {
      case geohashPattern() => true
      case _ => false
    }
  }

  private def decodeGeohash(hash: String): Option[GeoPoint] = {
    Some(GeoPoint(Geohash.decodeLatitude(hash), Geohash.decodeLongitude(hash)))
  }

  private def parseWKT(wkt: String): Option[GeoPoint] = {
    val reader = new WKTReader()
    Try {
      val geom = reader.read(wkt)
      geom match {
        case point: Point => GeoPoint(point.getY, point.getX)
        case _ => null
      }
    }.toOption
  }

}
