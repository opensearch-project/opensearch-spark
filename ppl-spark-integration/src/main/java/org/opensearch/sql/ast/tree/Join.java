/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

public class Join extends UnresolvedPlan {
    private UnresolvedPlan left;
    private final UnresolvedPlan right;
    private final String leftAlias;
    private final String rightAlias;
    private final JoinType joinType;
    private final UnresolvedExpression joinCondition;
    private final JoinHint joinHint;

    public Join(
            UnresolvedPlan right,
            String leftAlias,
            String rightAlias,
            JoinType joinType,
            UnresolvedExpression joinCondition,
            JoinHint joinHint) {
        this.right = right;
        this.leftAlias = leftAlias;
        this.rightAlias = rightAlias;
        this.joinType = joinType;
        this.joinCondition = joinCondition;
        this.joinHint = joinHint;
    }

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.left = new SubqueryAlias(leftAlias, child);
        return this;
    }

    @Override
    public List<UnresolvedPlan> getChild() {
        return ImmutableList.of(left);
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitJoin(this, context);
    }

    public enum JoinType {
        INNER,
        LEFT,
        RIGHT,
        SEMI,
        ANTI,
        CROSS,
        FULL
    }

    public UnresolvedPlan getRight() {
        return right;
    }

    public String getLeftAlias() {
        return leftAlias;
    }

    public String getRightAlias() {
        return rightAlias;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public UnresolvedExpression getJoinCondition() {
        return joinCondition;
    }

    public JoinHint getJoinHint() {
        return joinHint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Join join = (Join) o;
        return Objects.equals(left, join.left) && Objects.equals(right, join.right) && Objects.equals(leftAlias, join.leftAlias) && Objects.equals(rightAlias, join.rightAlias) && joinType == join.joinType && Objects.equals(joinCondition, join.joinCondition) && Objects.equals(joinHint, join.joinHint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, leftAlias, rightAlias, joinType, joinCondition, joinHint);
    }

    public static class JoinHint {
        private final Map<String, String> hints;

        public JoinHint() {
            this.hints = ImmutableMap.of();
        }

        public JoinHint(Map<String, String> hints) {
            this.hints = hints;
        }

        public Map<String, String> getHints() {
            return hints;
        }
    }
}