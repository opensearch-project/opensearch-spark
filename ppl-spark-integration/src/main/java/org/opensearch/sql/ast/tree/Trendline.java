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

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

@ToString
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Trendline extends UnresolvedPlan {

    private UnresolvedPlan child;
    @Nullable
    private final Field sortByField;
    private final List<Trendline.TrendlineComputation> computations;

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

    public List<Trendline.TrendlineComputation> filterComputationByType(TrendlineType type) {
        return computations.stream()
                .filter(computation -> computation.getComputationType().equals(type))
                .collect(Collectors.toList());
    }

    @Getter
    public static class TrendlineComputation extends UnresolvedExpression {

        private final Integer numberOfDataPoints;
        private final Field dataField;
        private final String alias;
        private final TrendlineType computationType;

        public TrendlineComputation(Integer numberOfDataPoints, Field dataField, String alias, String computationType) {
            this.numberOfDataPoints = numberOfDataPoints;
            this.dataField = dataField;
            this.alias = alias;
            this.computationType = Trendline.TrendlineType.valueOf(computationType.toUpperCase());
        }

        @Override
        public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
            return nodeVisitor.visitTrendlineComputation(this, context);
        }

    }

    public enum TrendlineType {
        SMA,
        WMA
    }
}
