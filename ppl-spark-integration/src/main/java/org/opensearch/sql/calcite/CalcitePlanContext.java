/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import lombok.Getter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.function.BiFunction;

@Getter
public class CalcitePlanContext {

    private RelBuilder relBuilder;
    private RexBuilder rexBuilder;

    private boolean isResolvingJoinCondition = false;

    public CalcitePlanContext(RelBuilder relBuilder) {
        this.relBuilder = relBuilder;
        this.rexBuilder = relBuilder.getRexBuilder();
    }

    public RexNode resolveJoinCondition(
        UnresolvedExpression expr,
        BiFunction<UnresolvedExpression, CalcitePlanContext, RexNode> transformFunction) {
        isResolvingJoinCondition = true;
        RexNode result = transformFunction.apply(expr, this);
        isResolvingJoinCondition = false;
        return result;
    }

    public static CalcitePlanContext create(FrameworkConfig config) {
        return new CalcitePlanContext(RelBuilder.create(config));
    }
}
