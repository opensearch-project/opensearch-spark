/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Field;

import java.util.List;

import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_FIRST;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_LAST;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.ASC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.DESC;

/**
 * AST node for Sort {@link Sort#sortList} represent a list of sort expression and sort options.
 */


public class Sort extends UnresolvedPlan {
    private UnresolvedPlan child;
    private List<Field> sortList;

    public Sort(List<Field> sortList) {
        this.sortList = sortList;
    }
    public Sort(UnresolvedPlan child, List<Field> sortList) {
        this.child = child;
        this.sortList = sortList;
    }

    @Override
    public Sort attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }

    @Override
    public List<UnresolvedPlan> getChild() {
        return ImmutableList.of(child);
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitSort(this, context);
    }

    public List<Field> getSortList() {
        return sortList;
    }

    /**
     * Sort Options.
     */

    public static class SortOption {

        /**
         * Default ascending sort option, null first.
         */
        public static SortOption DEFAULT_ASC = new SortOption(ASC, NULL_FIRST);

        /**
         * Default descending sort option, null last.
         */
        public static SortOption DEFAULT_DESC = new SortOption(DESC, NULL_LAST);

        private SortOrder sortOrder;
        private NullOrder nullOrder;

        public SortOption(SortOrder sortOrder, NullOrder nullOrder) {
            this.sortOrder = sortOrder;
            this.nullOrder = nullOrder;
        }
    }

    public enum SortOrder {
        ASC,
        DESC
    }

    public enum NullOrder {
        NULL_FIRST,
        NULL_LAST
    }
}
