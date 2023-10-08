package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opensearch.sql.ast.tree.Correlation;
import scala.collection.Seq;

public interface JoinSpecTransformer {

    static LogicalPlan join(Correlation.CorrelationType correlationType, Seq<Expression> fields, Expression valueExpression, Seq<Expression> mapping, LogicalPlan p) {
        //create a join statement
        return p;
    }
}