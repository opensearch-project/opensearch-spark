/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.iceberg

import org.opensearch.flint.spark.FlintSparkCoveringIndexSqlITSuite

class FlintSparkIcebergCoveringIndexITSuite
    extends FlintSparkCoveringIndexSqlITSuite
    with FlintSparkIcebergSuite {}
