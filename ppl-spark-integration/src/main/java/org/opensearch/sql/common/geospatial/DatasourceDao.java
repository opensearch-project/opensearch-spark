package org.opensearch.sql.common.geospatial;

import javafx.util.Pair;

import java.util.stream.Stream;

public interface DatasourceDao extends AutoCloseable {
    public Stream<Pair<String, GeoIpCache>> getGeoIps(String datasource);
}
