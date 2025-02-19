/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.udt

import org.apache.spark.sql.types.{SQLUserDefinedType, UserDefinedType}

@SQLUserDefinedType(udt = classOf[GeoPointUDT])
case class GeoPoint(lat: Double, lon: Double) {}
