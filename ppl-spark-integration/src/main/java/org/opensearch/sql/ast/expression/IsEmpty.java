package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;

public class IsEmpty extends UnresolvedExpression {
    private Case caseValue;

    public IsEmpty(Case caseValue) {
        this.caseValue = caseValue;
    }

    @Override
    public List<UnresolvedExpression> getChild() {
        return ImmutableList.of(this.caseValue);
    }

    @Override
    public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
        return nodeVisitor.visitIsEmpty(this, context);
    }

    public Case getCaseValue() {
        return caseValue;
    }

    @Override
    public String toString() {
        return String.format(
                "isempty(%s)",
                caseValue.toString());
    }
}
