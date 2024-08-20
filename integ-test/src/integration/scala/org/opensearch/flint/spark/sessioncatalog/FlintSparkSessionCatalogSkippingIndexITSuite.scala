/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sessioncatalog

import org.opensearch.flint.spark.FlintSparkSkippingIndexSqlITSuite

/**
 * Test Skipping Index with FlintDelegatingSessionCatalog.
 */
class FlintSparkSessionCatalogSkippingIndexITSuite
    extends FlintSparkSkippingIndexSqlITSuite
    with FlintSessionCatalogSuite {}
