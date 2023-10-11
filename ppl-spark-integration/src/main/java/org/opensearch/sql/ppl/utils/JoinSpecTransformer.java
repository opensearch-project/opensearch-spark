package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.FullOuter$;
import org.apache.spark.sql.catalyst.plans.Inner$;
import org.apache.spark.sql.catalyst.plans.JoinType;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.JoinHint;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.tree.Correlation;
import org.opensearch.sql.ppl.CatalystPlanContext;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;
import java.util.Optional;
import java.util.Stack;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static scala.collection.JavaConverters.seqAsJavaListConverter;

public interface JoinSpecTransformer {

    /**
     * @param correlationType the correlation type which can be exact (inner join) or approximate (outer join)
     * @param fields          - fields (columns) that needed to be joined by
     * @param scope           - this is a time base expression that timeframes the join to a specific period : (Time-field-name, value, unit)
     * @param mapping         - in case fields in different relations have different name, that can be aliased with the following names
     * @param context         - parent context including the plan to evolve to join with
     * @return
     */
    static LogicalPlan join(Correlation.CorrelationType correlationType, Seq<Expression> fields, Expression scope, Seq<Expression> mapping, CatalystPlanContext context) {
        //create a join statement - which will replace all the different plans with a single plan which contains the joined plans
        switch (correlationType) {
            case self:
                //expecting exactly one source relation
                if (context.getPlanBranches().size() != 1)
                    throw new IllegalStateException("Correlation command with `inner` type must have exactly on source table ");
                break;
            case exact:
                //expecting at least two source relations
                if (context.getPlanBranches().size() < 2)
                    throw new IllegalStateException("Correlation command with `exact` type must at least two source tables ");
                break;
            case approximate:
                if (context.getPlanBranches().size() < 2)
                    throw new IllegalStateException("Correlation command with `approximate` type must at least two source tables ");
                //expecting at least two source relations
                break;
        }

        // Define join condition
        Expression joinCondition = buildJoinCondition(seqAsJavaListConverter(fields).asJava(), seqAsJavaListConverter(mapping).asJava(), correlationType);
        // extract the plans from the context
        List<LogicalPlan> logicalPlans = seqAsJavaListConverter(context.retainAllPlans(p -> p)).asJava();
        // Define join step instead on the multiple query branches
        return context.with(logicalPlans.stream().reduce((left, right)
                -> new Join(left, right, getType(correlationType), Option.apply(joinCondition), JoinHint.NONE())).get());
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
}