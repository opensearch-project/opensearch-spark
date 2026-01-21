/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.ast.expression;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.flint.spark.ppl.legacy.ast.AbstractNodeVisitor;

import java.util.Arrays;
import java.util.List;

/** AST node that represents CIDR function. */
@AllArgsConstructor
@Getter
@EqualsAndHashCode(callSuper = false)
@ToString
public class Cidr extends UnresolvedExpression {
    private UnresolvedExpression ipAddress;
    private UnresolvedExpression cidrBlock;

    @Override
    public List<UnresolvedExpression> getChild() {
        return Arrays.asList(ipAddress, cidrBlock);
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitCidr(this, context);
    }
}
