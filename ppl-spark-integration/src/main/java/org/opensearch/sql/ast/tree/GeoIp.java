package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.Arrays;
import java.util.List;

@ToString
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class GeoIp extends UnresolvedPlan {
    private UnresolvedPlan child;
    private final UnresolvedExpression datasource;
    private final UnresolvedExpression ipAddress;
    private final UnresolvedExpression properties;

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
