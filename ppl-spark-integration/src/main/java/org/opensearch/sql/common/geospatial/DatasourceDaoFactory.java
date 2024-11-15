/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.geospatial;
import java.net.MalformedURLException;

public class DatasourceDaoFactory {
    public static DatasourceDao GetDatasourceDao(String datasource) {
        try {
            return new ManifestDao(datasource);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Invalid URL provided: " + datasource, e);
        }
    }
}
