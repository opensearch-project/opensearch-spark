/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.ast.tree;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.flint.spark.ppl.legacy.ast.AbstractNodeVisitor;
import org.opensearch.flint.spark.ppl.legacy.ast.expression.QualifiedName;
import org.opensearch.flint.spark.ppl.legacy.ast.expression.UnresolvedExpression;

import java.util.List;

/**
 * AST Node for Table Function.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class TableFunction extends UnresolvedPlan {

    private final UnresolvedExpression functionName;

    @Getter private final List<UnresolvedExpression> arguments;

    public QualifiedName getFunctionName() {
        return (QualifiedName) functionName;
    }

    @Override
    public List<UnresolvedPlan> getChild() {
        return ImmutableList.of();
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitTableFunction(this, context);
    }

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        return null;
    }
}
