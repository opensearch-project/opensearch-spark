package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.FieldsMapping;
import org.opensearch.sql.ast.expression.Scope;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.List;

/** Logical plan node of correlation , the interface for building the searching sources. */

public class Correlation extends UnresolvedPlan {
    private final CorrelationType correlationType;  
    private final List<UnresolvedExpression> fieldsList;
    private final Scope scope;
    private final FieldsMapping mappingListContext;
    private UnresolvedPlan child    ;
    public Correlation(String correlationType, List<UnresolvedExpression> fieldsList, Scope scope, FieldsMapping mappingListContext) {
        this.correlationType = CorrelationType.valueOf(correlationType);
        this.fieldsList = fieldsList;
        this.scope = scope;
        this.mappingListContext = mappingListContext;
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitCorrelation(this, context);
    }

    @Override
    public List<? extends Node> getChild() {
        return ImmutableList.of(child);
    }

    @Override
    public Correlation attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }

    public CorrelationType getCorrelationType() {
        return correlationType;
    }

    public List<UnresolvedExpression> getFieldsList() {
        return fieldsList;
    }

    public Scope getScope() {
        return scope;
    }

    public FieldsMapping getMappingListContext() {
        return mappingListContext;
    }

    public enum CorrelationType {
        exact,
        approximate
    }

    
}
