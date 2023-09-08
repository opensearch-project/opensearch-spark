package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GreaterThan;
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual;
import org.apache.spark.sql.catalyst.expressions.LessThan;
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual;
import org.apache.spark.sql.catalyst.expressions.Not;
import org.apache.spark.sql.catalyst.expressions.Predicate;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.ppl.CatalystPlanContext;

/**
 * Transform the PPL Logical comparator into catalyst comparator
 */
public interface ComparatorTransformer {
    /**
     * comparator expression builder building a catalyst binary comparator from PPL's compare logical step
     *
     * @return
     */
    static Predicate comparator(Compare expression, CatalystPlanContext context) {
        if (BuiltinFunctionName.of(expression.getOperator()).isEmpty())
            throw new IllegalStateException("Unexpected value: " + expression.getOperator());

        if (context.getNamedParseExpressions().isEmpty()) {
            throw new IllegalStateException("Unexpected value: No operands found in expression");
        }

        Expression right = context.getNamedParseExpressions().pop();
        Expression left = context.getNamedParseExpressions().isEmpty() ? null : context.getNamedParseExpressions().pop();

        // Additional function operators will be added here
        switch (BuiltinFunctionName.of(expression.getOperator()).get()) {
            case EQUAL:
                return new EqualTo(left, right);
            case NOTEQUAL:
                return new Not(new EqualTo(left, right));
            case LESS:
                return new LessThan(left, right);
            case LTE:
                return new LessThanOrEqual(left, right);
            case GREATER:
                return new GreaterThan(left, right);
            case GTE:
                return new GreaterThanOrEqual(left, right);
        }
        throw new IllegalStateException("Not Supported value: " + expression.getOperator());
    }
}
