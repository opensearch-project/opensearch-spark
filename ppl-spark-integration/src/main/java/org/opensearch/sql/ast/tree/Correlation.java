package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.FieldsMapping;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.Scope;

import java.util.List;

/** Logical plan node of correlation , the interface for building the searching sources. */
@ToString
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Correlation extends UnresolvedPlan {
    private final CorrelationType correlationType;
    private final List<QualifiedName> fieldsList;
    private final Scope scope;
    private final FieldsMapping mappingListContext;
    private UnresolvedPlan child    ;
    public Correlation(String correlationType, List<QualifiedName> fieldsList, Scope scope, FieldsMapping mappingListContext) {
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

    public enum CorrelationType {
        self,
        exact,
        approximate
    }
}
