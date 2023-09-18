package org.opensearch.sql.ast.tree;

import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** Logical plan node of Join, the interface for building the searching sources. */
public class JoinClause extends Filter {
    private UnresolvedExpression rightExpression;
    private UnresolvedExpression leftExpression;

    public JoinClause(UnresolvedExpression leftPart, UnresolvedExpression rightPart, UnresolvedExpression whereCommand) {
        super(whereCommand);
        this.leftExpression = leftPart;
        this.rightExpression = rightPart;
    }

    public UnresolvedExpression getRightExpression() {
        return rightExpression;
    }

    public UnresolvedExpression getLeftExpression() {
        return leftExpression;
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitJoin(this, context);
    }

}
