package org.opensearch.sql.ast.expression;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Scope expression node. Params include field expression and the scope value. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class Scope extends Span {
    public Scope(UnresolvedExpression field, UnresolvedExpression value, SpanUnit unit) {
        super(field, value, unit);
    }
}
