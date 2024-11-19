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

import java.util.Locale;

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

    public Row getRow(String[] properties) {
        if (properties == null || properties.length == 0) {
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
        } else {
            return RowFactory.create(getRowValues(properties));
        }
    }

    private Object[] getRowValues(String[] properties) {
        Object[] rowValues = new String[properties.length];
        for (int i = 0; i < properties.length; i++) {
            switch (properties[i].toUpperCase(Locale.ROOT)) {
            case "COUNTRY_ISO_CODE":
                rowValues[i] = country_iso_code;
                break;
            case "COUNTRY_NAME":
                rowValues[i] = country_name;
                break;
            case "CONTINENT_NAME":
                rowValues[i] = continent_name;
                break;
            case "REGION_ISO_CODE":
                rowValues[i] = region_iso_code;
                break;
            case "REGION_NAME":
                rowValues[i] = region_name;
                break;
            case "CITY_NAME":
                rowValues[i] = city_name;
                break;
            case "TIME_ZONE":
                rowValues[i] = time_zone;
                break;
            case "LAT":
                rowValues[i] = lat;
                break;
            case "LON":
                rowValues[i] = lon;
                break;
            }
        }

        return rowValues;
    }
}
