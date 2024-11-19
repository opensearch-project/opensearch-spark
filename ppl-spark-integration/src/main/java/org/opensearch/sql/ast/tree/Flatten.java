package org.opensearch.sql.ast.tree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Field;

import java.util.List;

@RequiredArgsConstructor
public class Flatten extends UnresolvedPlan {

    private UnresolvedPlan child;

    @Getter
    private final Field field;

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }

    @Override
    public List<? extends Node> getChild() {
        return child == null ? List.of() : List.of(child);
    }
    
    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitFlatten(this, context);
    }
}
