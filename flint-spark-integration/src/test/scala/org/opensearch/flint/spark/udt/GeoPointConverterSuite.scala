/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.udt

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GeoPointConverterSuite extends AnyFlatSpec with Matchers {

  val jsonFactory = new JsonFactory()

  def parseJson(json: String): JsonParser = {
    val parser = jsonFactory.createParser(json)
    parser.nextToken() // Advance to the first token
    parser
  }

  "GeoPointConverter" should "parse string format 'lat,lon'" in {
    val parser = parseJson("\"47.6062,-122.3321\"")
    val result = GeoPointConverter.fromJsonParser(parser)
    result shouldBe Some(GeoPoint(47.6062, -122.3321))
  }

  it should "parse array format [lon, lat]" in {
    val parser = parseJson("[-122.3321, 47.6062]")
    val result = GeoPointConverter.fromJsonParser(parser)
    result shouldBe Some(GeoPoint(47.6062, -122.3321))
  }

  it should "parse object with 'lat' and 'lon' fields" in {
    val parser = parseJson("""{"lat": 47.6062, "lon": -122.3321}""")
    val result = GeoPointConverter.fromJsonParser(parser)
    result shouldBe Some(GeoPoint(47.6062, -122.3321))
  }

  it should "parse GeoJSON Point object" in {
    val parser = parseJson("""{"type": "Point", "coordinates": [-122.3321, 47.6062]}""")
    val result = GeoPointConverter.fromJsonParser(parser)
    result shouldBe Some(GeoPoint(47.6062, -122.3321))
  }

  it should "parse WKT Point string" in {
    val parser = parseJson("\"POINT (-122.3321 47.6062)\"")
    val result = GeoPointConverter.fromJsonParser(parser)
    result shouldBe Some(GeoPoint(47.6062, -122.3321))
  }

  it should "return None for invalid string" in {
    val parser = parseJson("\"invalid\"")
    val result = GeoPointConverter.fromJsonParser(parser)
    result shouldBe None
  }

  it should "return None for incomplete object" in {
    val parser = parseJson("""{"lat": 47.6062}""")
    val result = GeoPointConverter.fromJsonParser(parser)
    result shouldBe None
  }

  it should "return None for invalid array" in {
    val parser = parseJson("[47.6062]")
    val result = GeoPointConverter.fromJsonParser(parser)
    result shouldBe None
  }
}
