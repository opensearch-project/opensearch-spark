/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Arrays;
import java.util.List;

public class Cidr extends UnresolvedExpression {
    private UnresolvedExpression ipAddress;
    private UnresolvedExpression cidrBlock;

    public Cidr(UnresolvedExpression ipAddress, UnresolvedExpression cidrBlock) {
        this.ipAddress = ipAddress;
        this.cidrBlock = cidrBlock;
    }

    public UnresolvedExpression getCidrBlock() {
        return cidrBlock;
    }

    public UnresolvedExpression getIpAddress() {
        return ipAddress;
    }

    @Override
    public List<UnresolvedExpression> getChild() {
        return Arrays.asList(ipAddress, cidrBlock);
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitCidr(this, context);
    }

    @Override
    public String toString() {
        return String.format(
                "CIDR(%s,%s)",
                ipAddress.toString(), cidrBlock.toString());
    }
}
