/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

import java.util.Locale;

public interface CalciteHelper {

    static SqlOperator translate(String op) {
        switch (op.toUpperCase(Locale.ROOT)) {
            case "AND":
                return SqlStdOperatorTable.AND;
            case "OR":
                return SqlStdOperatorTable.OR;
            case "NOT":
                return SqlStdOperatorTable.NOT;
            case "XOR":
                return SqlStdOperatorTable.BIT_XOR;
            case "=":
                return SqlStdOperatorTable.EQUALS;
            case "<>":
            case "!=":
                return SqlStdOperatorTable.NOT_EQUALS;
            case ">":
                return SqlStdOperatorTable.GREATER_THAN;
            case ">=":
                return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
            case "<":
                return SqlStdOperatorTable.LESS_THAN;
            case "<=":
                return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
            case "+":
                return SqlStdOperatorTable.PLUS;
            case "-":
                return SqlStdOperatorTable.MINUS;
            case "*":
                return SqlStdOperatorTable.MULTIPLY;
            case "/":
                return SqlStdOperatorTable.DIVIDE;
            // Built-in String Functions
            case "LOWER":
                return SqlStdOperatorTable.LOWER;
            case "LIKE":
                return SqlStdOperatorTable.LIKE;
            // Built-in Math Functions
            case "ABS":
                return SqlStdOperatorTable.ABS;
            // Built-in Date Functions
            case "CURRENT_TIMESTAMP":
                return SqlStdOperatorTable.CURRENT_TIMESTAMP;
            case "CURRENT_DATE":
                return SqlStdOperatorTable.CURRENT_DATE;
            case "DATE":
                return SqlLibraryOperators.DATE;
            case "ADDDATE":
                return SqlLibraryOperators.DATE_ADD_SPARK;
            case "DATE_ADD":
                return SqlLibraryOperators.DATEADD;
            // TODO Add more, ref RexImpTable
            default:
                throw new IllegalArgumentException("Unsupported operator: " + op);
        }
    }

    static RelBuilder.AggCall translate(AggregateFunction agg, RexNode field, CalcitePlanContext context) {
        if (BuiltinFunctionName.ofAggregation(agg.getFuncName()).isEmpty())
            throw new IllegalStateException("Unexpected value: " + agg.getFuncName());

        // Additional aggregation function operators will be added here
        BuiltinFunctionName functionName = BuiltinFunctionName.ofAggregation(agg.getFuncName()).get();
        switch (functionName) {
            case MAX:
                return context.getRelBuilder().max(field);
            case MIN:
                return context.getRelBuilder().min(field);
            case MEAN:
                throw new UnsupportedOperationException("MEAN is not supported in PPL");
            case AVG:
                return context.getRelBuilder().avg(agg.getDistinct(), null, field);
            case COUNT:
                return context.getRelBuilder().count(agg.getDistinct(), null, field == null ? ImmutableList.of() : ImmutableList.of(field));
            case SUM:
                return context.getRelBuilder().sum(agg.getDistinct(), null, field);
            case STDDEV:
                return context.getRelBuilder().aggregateCall(SqlStdOperatorTable.STDDEV, field);
            case STDDEV_POP:
                return context.getRelBuilder().aggregateCall(SqlStdOperatorTable.STDDEV_POP, field);
            case STDDEV_SAMP:
                return context.getRelBuilder().aggregateCall(SqlStdOperatorTable.STDDEV_SAMP, field);
            case PERCENTILE:
                return context.getRelBuilder().aggregateCall(SqlStdOperatorTable.PERCENTILE_CONT, field);
            case PERCENTILE_APPROX:
                throw new UnsupportedOperationException("PERCENTILE_APPROX is not supported in PPL");
            case APPROX_COUNT_DISTINCT:
                return context.getRelBuilder().aggregateCall(SqlStdOperatorTable.APPROX_COUNT_DISTINCT, field);
        }
        throw new IllegalStateException("Not Supported value: " + agg.getFuncName());
    }

    static AggregateCall translateAggregateCall(AggregateFunction agg, RexNode field, RelBuilder relBuilder) {
        if (BuiltinFunctionName.ofAggregation(agg.getFuncName()).isEmpty())
            throw new IllegalStateException("Unexpected value: " + agg.getFuncName());

        // Additional aggregation function operators will be added here
        BuiltinFunctionName functionName = BuiltinFunctionName.ofAggregation(agg.getFuncName()).get();
        boolean isDistinct = agg.getDistinct();
        switch (functionName) {
            case MAX:
                return aggCreate(SqlStdOperatorTable.MAX, isDistinct, field);
            case MIN:
                return aggCreate(SqlStdOperatorTable.MIN, isDistinct, field);
            case MEAN:
                throw new UnsupportedOperationException("MEAN is not supported in PPL");
            case AVG:
                return aggCreate(SqlStdOperatorTable.AVG, isDistinct, field);
            case COUNT:
                return aggCreate(SqlStdOperatorTable.COUNT, isDistinct, field);
            case SUM:
                return aggCreate(SqlStdOperatorTable.SUM, isDistinct, field);
        }
        throw new IllegalStateException("Not Supported value: " + agg.getFuncName());
    }

    static AggregateCall aggCreate(SqlAggFunction agg, boolean isDistinct, RexNode field) {
        int index = ((RexInputRef) field).getIndex();
        return AggregateCall.create(agg, isDistinct, false, false, ImmutableList.of(), ImmutableList.of(index), -1, null, RelCollations.EMPTY, field.getType(), null);
    }

    static JoinRelType translateJoinType(Join.JoinType joinType) {
        switch (joinType) {
            case LEFT:
                return JoinRelType.LEFT;
            case RIGHT:
                return JoinRelType.RIGHT;
            case FULL:
                return JoinRelType.FULL;
            case SEMI:
                return JoinRelType.SEMI;
            case ANTI:
                return JoinRelType.ANTI;
            case INNER:
            default:
                return JoinRelType.INNER;
        }
    }
}
