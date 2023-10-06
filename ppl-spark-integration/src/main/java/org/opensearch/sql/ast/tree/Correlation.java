package org.opensearch.sql.ast.tree;

import org.opensearch.flint.spark.ppl.OpenSearchPPLParser;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;

/** Logical plan node of correlation , the interface for building the searching sources. */

public class Correlation extends UnresolvedPlan {
    private final CorrelationType correlationTypeContext;
    private final List<OpenSearchPPLParser.FieldExpressionContext> fieldExpression;
    private final OpenSearchPPLParser.ScopeClauseContext contextParamContext;
    private final OpenSearchPPLParser.MappingListContext mappingListContext;
    private UnresolvedPlan child;
    public Correlation(OpenSearchPPLParser.CorrelationTypeContext correlationTypeContext, OpenSearchPPLParser.FieldListContext fieldListContext, OpenSearchPPLParser.ScopeClauseContext contextParamContext, OpenSearchPPLParser.MappingListContext mappingListContext) {
        this.correlationTypeContext = CorrelationType.valueOf(correlationTypeContext.getText());
        this.fieldExpression = fieldListContext.fieldExpression();
        this.contextParamContext = contextParamContext;
        this.mappingListContext = mappingListContext;
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitCorrelation(this, context);
    }

    @Override
    public Correlation attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }
    
    enum CorrelationType {
        exact,
        approximate
    }

}
