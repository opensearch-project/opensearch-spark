package org.opensearch.sql.common.geospatial;

import lombok.*;

@Getter
@ToString
@AllArgsConstructor
public class GeoIpData {
    private final String country_is_code;
    private final String country_name;
    private final String continent_name;
    private final String region_iso_code;
    private final String region_name;
    private final String city_name;
    private final String time_zone;
    private final String lat;
    private final String lon;
}
