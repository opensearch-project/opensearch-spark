/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.ast.tree;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.flint.spark.ppl.legacy.ast.AbstractNodeVisitor;
import org.opensearch.flint.spark.ppl.legacy.ast.Node;
import org.opensearch.flint.spark.ppl.legacy.ast.expression.Argument;
import org.opensearch.flint.spark.ppl.legacy.ast.expression.AttributeList;
import org.opensearch.flint.spark.ppl.legacy.ast.expression.UnresolvedExpression;

import java.util.List;

@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class FieldSummary extends UnresolvedPlan {
    private List<UnresolvedExpression> includeFields;
    private boolean includeNull;
    private List<UnresolvedExpression> collect;
    private UnresolvedPlan child;

    public FieldSummary(List<UnresolvedExpression> collect) {
        this.collect = collect;
        collect.forEach(exp -> {
                    if (exp instanceof Argument) {
                        this.includeNull = (boolean) ((Argument)exp).getValue().getValue();
                    }
                    if (exp instanceof AttributeList) {
                        this.includeFields = ((AttributeList)exp).getAttrList();
                    }
        });
    }


    @Override
    public List<? extends Node> getChild() {
        return child == null ? List.of() : List.of(child);
    }

    @Override
    public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
        return nodeVisitor.visitFieldSummary(this, context);
    }

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }

}
