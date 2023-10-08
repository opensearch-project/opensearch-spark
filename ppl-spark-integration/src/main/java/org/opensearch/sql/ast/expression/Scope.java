package org.opensearch.sql.ast.expression;

/** Scope expression node. Params include field expression and the scope value. */
public class Scope extends Span {
    public Scope(UnresolvedExpression field, UnresolvedExpression value, SpanUnit unit) {
        super(field, value, unit);
    }
    
}
