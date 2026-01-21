/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.ppl.utils;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.Cross$;
import org.apache.spark.sql.catalyst.plans.FullOuter$;
import org.apache.spark.sql.catalyst.plans.Inner$;
import org.apache.spark.sql.catalyst.plans.JoinType;
import org.apache.spark.sql.catalyst.plans.LeftAnti$;
import org.apache.spark.sql.catalyst.plans.LeftOuter$;
import org.apache.spark.sql.catalyst.plans.LeftSemi$;
import org.apache.spark.sql.catalyst.plans.RightOuter$;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.JoinHint;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opensearch.flint.spark.ppl.legacy.ast.tree.Correlation;
import scala.Option;
import scala.collection.Seq;

import java.util.List;
import java.util.Optional;

import static scala.collection.JavaConverters.seqAsJavaListConverter;

public interface JoinSpecTransformer {

    /**
     * @param correlationType the correlation type which can be exact (inner join) or approximate (outer join)
     * @param fields          - fields (columns) that needed to be joined by
     * @param mapping         - in case fields in different relations have different name, that can be aliased with the following names
     * @return
     */
    static LogicalPlan join(Correlation.CorrelationType correlationType, Seq<Expression> fields, Seq<Expression> mapping, LogicalPlan left, LogicalPlan right) {
        //create a join statement - which will replace all the different plans with a single plan which contains the joined plans
        switch (correlationType) {
            case self:
                //expecting exactly one source relation
                if (!left.equals(right))
                    throw new IllegalStateException("Correlation command with `inner` type must have exactly on source table ");
                break;
            case exact:
                //expecting at least two source relations
                if (left.equals(right))
                    throw new IllegalStateException("Correlation command with `exact` type must at least two different source tables ");
                break;
            case approximate:
                if (left.equals(right))
                    throw new IllegalStateException("Correlation command with `approximate` type must at least two different source tables ");
                //expecting at least two source relations
                break;
        }

        if (fields.isEmpty())
            throw new IllegalStateException("Correlation command was called with `empty` correlation fields ");

        if (mapping.isEmpty())
            throw new IllegalStateException("Correlation command was called with `empty` correlation mappings ");

        if (mapping.seq().size() != fields.seq().size())
            throw new IllegalStateException("Correlation command was called with `fields` attribute having different elements from the 'mapping' attributes ");

        // Define join condition
        Expression joinCondition = buildJoinCondition(seqAsJavaListConverter(fields).asJava(), seqAsJavaListConverter(mapping).asJava(), correlationType);
        // Define join step instead on the multiple query branches
        return new Join(left, right, getType(correlationType), Option.apply(joinCondition), JoinHint.NONE());
    }

    static Expression buildJoinCondition(List<Expression> fields, List<Expression> mapping, Correlation.CorrelationType correlationType) {
        switch (correlationType) {
            case self:
                //expecting exactly one source relation - mapping will be used to set the inner join counterpart
                break;
            case exact:
                //expecting at least two source relations
                return mapping.stream().reduce(org.apache.spark.sql.catalyst.expressions.And::new).orElse(null);
            case approximate:
                return mapping.stream().reduce(org.apache.spark.sql.catalyst.expressions.Or::new).orElse(null);
        }
        return mapping.stream().reduce(org.apache.spark.sql.catalyst.expressions.And::new).orElse(null);
    }

    static JoinType getType(Correlation.CorrelationType correlationType) {
        switch (correlationType) {
            case self:
            case exact:
                return Inner$.MODULE$;
            case approximate:
                return FullOuter$.MODULE$;
        }
        return Inner$.MODULE$;
    }

    // +----------------------------------------------------------------------------+
    // | Native Join Syntax (https://github.com/opensearch-project/sql/issues/2913) |
    // +----------------------------------------------------------------------------+

    static LogicalPlan join(LogicalPlan left, LogicalPlan right, org.opensearch.flint.spark.ppl.legacy.ast.tree.Join.JoinType joinType, Optional<Expression> joinCondition, org.opensearch.flint.spark.ppl.legacy.ast.tree.Join.JoinHint joinHint) {
        return new Join(left, right, getType(joinType), Option.apply(joinCondition.orElse(null)), JoinHint.NONE());
    }

    static JoinType getType(org.opensearch.flint.spark.ppl.legacy.ast.tree.Join.JoinType joinType) {
        switch (joinType) {
            case INNER:
                return Inner$.MODULE$;
            case LEFT:
                return LeftOuter$.MODULE$;
            case RIGHT:
                return RightOuter$.MODULE$;
            case FULL:
                return FullOuter$.MODULE$;
            case SEMI:
                return LeftSemi$.MODULE$;
            case ANTI:
                return LeftAnti$.MODULE$;
            case CROSS:
                return Cross$.MODULE$;
        }
        return Inner$.MODULE$;
    }

}