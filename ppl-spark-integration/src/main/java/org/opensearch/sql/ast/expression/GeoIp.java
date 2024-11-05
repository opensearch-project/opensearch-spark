package org.opensearch.sql.ast.expression;

import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Arrays;
import java.util.List;

public class GeoIp extends UnresolvedExpression {
    private UnresolvedExpression datasource;
    private UnresolvedExpression ipAddress;
    private Literal properties;

    @Override
    public List<UnresolvedExpression> getChild() {
        return Arrays.asList(datasource, ipAddress);
    }

    @Override
    public <T,C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitGeoIp(this, context);
    }
}
