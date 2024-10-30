/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.List;
import java.util.Optional;

@ToString
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Trendline extends UnresolvedPlan {

    private UnresolvedPlan child;
    private final Optional<Field> sortByField;
    private final List<TrendlineComputation> computations;

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }

    @Override
    public List<? extends Node> getChild() {
        return ImmutableList.of(child);
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> visitor, C context) {
        return visitor.visitTrendline(this, context);
    }

    @Getter
    public static class TrendlineComputation {

        private final Integer numberOfDataPoints;
        private final UnresolvedExpression dataField;
        private final String alias;
        private final TrendlineType computationType;

        public TrendlineComputation(Integer numberOfDataPoints, UnresolvedExpression dataField, String alias, Trendline.TrendlineType computationType) {
            this.numberOfDataPoints = numberOfDataPoints;
            this.dataField = dataField;
            this.alias = alias;
            this.computationType = computationType;
        }

    }

    public enum TrendlineType {
        SMA
    }
}
