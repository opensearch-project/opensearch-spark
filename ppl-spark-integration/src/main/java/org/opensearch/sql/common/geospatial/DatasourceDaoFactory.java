/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.geospatial;

import java.lang.reflect.InvocationTargetException;

public class DatasourceDaoFactory {
    public static DatasourceDao GetDatasourceDao(String className, String datasource) {
        try {
            Class<?> dataSourceClass = Class.forName(className);

            if (!DatasourceDao.class.isAssignableFrom(dataSourceClass)) {
                throw new RuntimeException(className + " does not implement DatasourceDao");
            }

            return (DatasourceDao) dataSourceClass.getDeclaredConstructor().newInstance(datasource);
        } catch (Exception e){
            throw new RuntimeException("Could not instantiate datasource DAO: " + datasource, e);
        }
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
