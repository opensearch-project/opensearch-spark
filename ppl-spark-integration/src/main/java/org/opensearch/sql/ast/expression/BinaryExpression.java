package org.opensearch.sql.ast.expression;

import java.util.Arrays;
import java.util.List;

public abstract class BinaryExpression extends UnresolvedExpression {
    private UnresolvedExpression left;
    private UnresolvedExpression right;

    public BinaryExpression(UnresolvedExpression left, UnresolvedExpression right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public List<UnresolvedExpression> getChild() {
        return Arrays.asList(left, right);
    }

    public UnresolvedExpression getLeft() {
        return left;
    }

    public UnresolvedExpression getRight() {
        return right;
    }

}

