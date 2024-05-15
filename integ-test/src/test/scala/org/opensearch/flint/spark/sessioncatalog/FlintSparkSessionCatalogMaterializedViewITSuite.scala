/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sessioncatalog

import org.opensearch.flint.spark.FlintSparkMaterializedViewSqlITSuite

/**
 * Test MaterializedView with FlintDelegatingSessionCatalog.
 */
class FlintSparkSessionCatalogMaterializedViewITSuite
    extends FlintSparkMaterializedViewSqlITSuite
    with FlintSessionCatalogSuit {}
