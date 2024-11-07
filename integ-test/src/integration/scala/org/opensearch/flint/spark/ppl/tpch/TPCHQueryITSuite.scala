/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.tpch

import org.opensearch.flint.spark.ppl.LogicalPlanTestUtils

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.streaming.StreamTest

class TPCHQueryITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with TPCHQueryBase
    with StreamTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  tpchQueries.foreach { name =>
    val queryString = resourceToString(
      s"tpch/$name.ppl",
      classLoader = Thread.currentThread().getContextClassLoader)
    test(name) {
      // check the plans can be properly generated
      val plan = sql(queryString).queryExecution.executedPlan
      checkGeneratedCode(plan)
    }
  }
}
