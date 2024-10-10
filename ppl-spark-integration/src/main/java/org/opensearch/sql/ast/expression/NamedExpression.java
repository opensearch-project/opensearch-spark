package org.opensearch.sql.ast.expression;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Arrays;
import java.util.List;

@Getter
@ToString
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class NamedExpression extends UnresolvedExpression {
    private final int expressionId;
    private final UnresolvedExpression expression;

    //    private final DataType valueType;
    @Override
    public List<UnresolvedExpression> getChild() {
        return Arrays.asList(expression);
    }
    
    @Override
    public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
        return nodeVisitor.visit(this, context);
    }
}