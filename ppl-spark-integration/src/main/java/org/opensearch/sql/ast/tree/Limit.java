/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;

public class Limit extends UnresolvedPlan {
    private UnresolvedPlan child;
    private Integer limit;
    private Integer offset;

    public Limit(Integer limit, Integer offset) {
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public Limit attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }

    @Override
    public List<UnresolvedPlan> getChild() {
        return ImmutableList.of(this.child);
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> visitor, C context) {
        return visitor.visitLimit(this, context);
    }
}
