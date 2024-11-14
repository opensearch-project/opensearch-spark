/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.geospatial;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

@Getter
@ToString
@Builder
public class GeoIpData {
    private final String country_iso_code;
    private final String country_name;
    private final String continent_name;
    private final String region_iso_code;
    private final String region_name;
    private final String city_name;
    private final String time_zone;
    private final String lat;
    private final String lon;

    public Row getRow() {
        return RowFactory.create(
                country_iso_code,
                country_name,
                continent_name,
                region_iso_code,
                region_name,
                city_name,
                time_zone,
                lat,
                lon
        );
    }
}
