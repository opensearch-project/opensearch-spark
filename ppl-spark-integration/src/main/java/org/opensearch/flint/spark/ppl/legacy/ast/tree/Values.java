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
import org.opensearch.flint.spark.ppl.legacy.ast.Node;
import org.opensearch.flint.spark.ppl.legacy.ast.expression.Literal;

import java.util.List;

/**
 * AST node class for a sequence of literal values.
 */
@ToString
@Getter
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Values extends UnresolvedPlan {

    private final List<List<Literal>> values;

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        throw new UnsupportedOperationException("Values node is supposed to have no child node");
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitValues(this, context);
    }

    @Override
    public List<? extends Node> getChild() {
        return ImmutableList.of();
    }
}
