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
    private final UnresolvedPlan left;
    private final UnresolvedPlan right;
    private final JoinType joinType;
    private final UnresolvedExpression joinCondition;
    private final JoinHint joinHint;

    public Join(UnresolvedPlan left, UnresolvedPlan right, JoinType joinType, UnresolvedExpression joinCondition, JoinHint joinHint) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
        this.joinCondition = joinCondition;
        this.joinHint = joinHint;
    }

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        return this;
    }

    @Override
    public List<UnresolvedPlan> getChild() {
        return ImmutableList.of(left, right);
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

    public UnresolvedPlan getLeft() {
        return left;
    }

    public UnresolvedPlan getRight() {
        return right;
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
        return Objects.equals(left, join.left) && Objects.equals(right, join.right) && joinType == join.joinType && Objects.equals(joinCondition, join.joinCondition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, joinType, joinCondition);
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