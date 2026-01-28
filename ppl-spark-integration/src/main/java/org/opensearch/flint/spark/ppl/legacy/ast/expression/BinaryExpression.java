/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.ast.expression;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.List;

@Getter
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public abstract class BinaryExpression extends UnresolvedExpression {
    private final UnresolvedExpression left;
    private final UnresolvedExpression right;

    @Override
    public List<UnresolvedExpression> getChild() {
        return Arrays.asList(left, right);
    }
}
