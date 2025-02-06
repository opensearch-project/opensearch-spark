/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}

object PPLSparkUtils {

  def findLogicalRelations(plan: LogicalPlan): Seq[UnresolvedRelation] = {
    plan
      .transformDown { case relation: UnresolvedRelation =>
        relation
      }
      .collect { case relation: UnresolvedRelation =>
        relation
      }
  }
}
