/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, View}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * This analysis rule validates that the submitted query is not referencing a Glue Catalog view.
 * The rule simply traverses the plan and validates that none of the nodes resolved to a [[View]].
 */
class DisallowCatalogViews extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.foreachUp {
      case _: View =>
        throw new IllegalArgumentException(s"Catalog View is not allowed to be queried")

      case other => other
    }
    plan
  }
}
