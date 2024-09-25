/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;
import java.util.Objects;

public class SubqueryAlias extends UnresolvedPlan {
    private final String alias;
    private UnresolvedPlan child;

    public SubqueryAlias(String alias, UnresolvedPlan child) {
        this.alias = alias;
        this.child = child;
    }

    /**
     * Create an alias (SubqueryAlias) for a sub-query with a default alias name
     */
    public SubqueryAlias(UnresolvedPlan child, String suffix) {
        this.alias = "__auto_generated_subquery_name" + suffix;
        this.child = child;
    }

    public String getAlias() {
        return alias;
    }

    public List<UnresolvedPlan> getChild() {
        return ImmutableList.of(child);
    }

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitSubqueryAlias(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubqueryAlias alias1 = (SubqueryAlias) o;
        return Objects.equals(alias, alias1.alias) && Objects.equals(child, alias1.child);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, child);
    }
}
