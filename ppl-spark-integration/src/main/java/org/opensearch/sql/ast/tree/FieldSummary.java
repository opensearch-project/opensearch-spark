/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.FieldList;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.NamedExpression;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.List;

import static org.opensearch.flint.spark.ppl.OpenSearchPPLParser.INCLUDEFIELDS;
import static org.opensearch.flint.spark.ppl.OpenSearchPPLParser.NULLS;
import static org.opensearch.flint.spark.ppl.OpenSearchPPLParser.TOPVALUES;

@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class FieldSummary extends UnresolvedPlan {
    private List<Field> includeFields;
    private int topValues;
    private boolean ignoreNull;
    private List<UnresolvedExpression> collect;
    private UnresolvedPlan child;

    public FieldSummary(List<UnresolvedExpression> collect) {
        this.collect = collect;
        collect.stream().filter(e->e instanceof NamedExpression)
                .forEach(exp -> {
            switch (((NamedExpression) exp).getExpressionId()) {
                case NULLS:
                    this.ignoreNull = (boolean) ((Literal) exp.getChild().get(0)).getValue();
                    break;
                case TOPVALUES:
                    this.topValues = (int) ((Literal) exp.getChild().get(0)).getValue();
                    break;
                case INCLUDEFIELDS:
                    this.includeFields = ((FieldList) exp.getChild().get(0)).getFieldList();
                    break;
            }
        });
    }


    @Override
    public List<? extends Node> getChild() {
        return child == null ? List.of() : List.of(child);
    }

    @Override
    public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
        return nodeVisitor.visitFieldSummary(this, context);
    }

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }

}
