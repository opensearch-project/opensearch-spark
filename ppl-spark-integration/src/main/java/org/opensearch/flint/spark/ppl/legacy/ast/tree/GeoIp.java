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
import org.opensearch.flint.spark.ppl.legacy.ast.expression.AttributeList;
import org.opensearch.flint.spark.ppl.legacy.ast.expression.Field;
import org.opensearch.flint.spark.ppl.legacy.ast.expression.UnresolvedExpression;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class GeoIp extends UnresolvedPlan {
    private UnresolvedPlan child;
    private final Field field;
    private final UnresolvedExpression ipAddress;
    private final AttributeList properties;

    @Override
    public List<? extends Node> getChild() {
        return ImmutableList.of(child);
    }

    @Override
    public <T,C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitGeoIp(this, context);
    }

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }
}