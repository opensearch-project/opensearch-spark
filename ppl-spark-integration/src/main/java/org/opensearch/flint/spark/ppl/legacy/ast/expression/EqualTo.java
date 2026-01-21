/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.ast.expression;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.flint.spark.ppl.legacy.ast.AbstractNodeVisitor;

import java.util.Arrays;
import java.util.List;

/** Expression node of binary operator or comparison relation EQUAL. */
@ToString
@Getter
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class EqualTo extends UnresolvedExpression {
    private final UnresolvedExpression left;
    private final UnresolvedExpression right;

    @Override
    public List<UnresolvedExpression> getChild() {
        return Arrays.asList(left, right);
    }

    @Override
    public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
        return nodeVisitor.visitEqualTo(this, context);
    }
}
