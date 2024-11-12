/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.geospatial;

import lombok.*;

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
}
