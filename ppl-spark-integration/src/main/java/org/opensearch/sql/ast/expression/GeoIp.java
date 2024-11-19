package org.opensearch.sql.ast.expression;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Arrays;
import java.util.List;

@Getter
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class GeoIp extends UnresolvedExpression {
    private final UnresolvedExpression datasource;
    private final UnresolvedExpression ipAddress;
    private final UnresolvedExpression properties;

    @Override
    public List<UnresolvedExpression> getChild() {
        return Arrays.asList(datasource, ipAddress);
    }

    @Override
    public <T,C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitGeoIp(this, context);
    }
}
