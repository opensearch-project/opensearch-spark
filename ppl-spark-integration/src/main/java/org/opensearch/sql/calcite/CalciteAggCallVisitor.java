/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import static org.opensearch.sql.calcite.CalciteHelper.translate;

public class CalciteAggCallVisitor extends AbstractNodeVisitor<AggCall, CalcitePlanContext> {
    private final CalciteRexNodeVisitor rexNodeVisitor;

    public CalciteAggCallVisitor(CalciteRexNodeVisitor rexNodeVisitor) {
        this.rexNodeVisitor = rexNodeVisitor;
    }

    public AggCall analyze(UnresolvedExpression unresolved, CalcitePlanContext context) {
        return unresolved.accept(this, context);
    }

    @Override
    public AggCall visitAlias(Alias node, CalcitePlanContext context) {
        AggCall aggCall = analyze(node.getDelegated(), context);
        return aggCall.as(node.getName());
    }

    @Override
    public AggCall visitAggregateFunction(AggregateFunction node, CalcitePlanContext context) {
        RexNode field = rexNodeVisitor.analyze(node.getField(), context);
        return translate(node, field, context);
    }
}
