package org.opensearch.sql.ast.tree;

import org.opensearch.sql.ast.expression.Literal;

import java.util.Optional;

public interface CountedAggregation {
    Optional<Literal> getResults();
}
