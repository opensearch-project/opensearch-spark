package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;

public class IsEmpty extends UnresolvedExpression {
    private When whenClause;
    private UnresolvedExpression elseClause;

    public IsEmpty(When whenClause, UnresolvedExpression elseClause) {
        this.whenClause = whenClause;
        this.elseClause = elseClause;
    }

    @Override
    public List<UnresolvedExpression> getChild() {
        return ImmutableList.of(this.whenClause, this.elseClause);
    }

    @Override
    public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
        return nodeVisitor.visitIsEmpty(this, context);
    }

    public When getWhenClause() {
        return whenClause;
    }

    public UnresolvedExpression getElseClause() {
        return elseClause;
    }

    @Override
    public String toString() {
        return String.format(
                "isempty(when %s else %s)",
                whenClause.toString(), elseClause.toString());
    }
}
