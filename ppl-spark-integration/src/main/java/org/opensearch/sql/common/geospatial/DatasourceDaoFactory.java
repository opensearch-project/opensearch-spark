package org.opensearch.sql.common.geospatial;

public class DatasourceDaoFactory {
    public static DatasourceDao GetDatasourceDaoFactory(String datasource) {
        return new TestDatasourceDao(datasource);
    }

    private static DatasourceType getDatasourceType(String datasource) {
        return DatasourceType.MANIFEST;
    }

    private enum DatasourceType {
        MANIFEST,
        API,
        INDEX
    }
}
