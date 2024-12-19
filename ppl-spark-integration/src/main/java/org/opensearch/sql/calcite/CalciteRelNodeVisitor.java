/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.SqlKind.AS;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_FIRST;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_LAST;
import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_DESC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.ASC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.DESC;
import static org.opensearch.sql.calcite.CalciteHelper.translateJoinType;

public class CalciteRelNodeVisitor extends AbstractNodeVisitor<Void, CalcitePlanContext> {

    private final CalciteRexNodeVisitor rexVisitor;
    private final CalciteAggCallVisitor aggVisitor;

    public CalciteRelNodeVisitor() {
        this.rexVisitor = new CalciteRexNodeVisitor();
        this.aggVisitor = new CalciteAggCallVisitor(rexVisitor);
    }

    public Void analyze(UnresolvedPlan unresolved, CalcitePlanContext context) {
        return unresolved.accept(this, context);
    }

    @Override
    public Void visitRelation(Relation node, CalcitePlanContext context) {
        for (QualifiedName qualifiedName : node.getQualifiedNames()) {
            context.getRelBuilder().scan(qualifiedName.getParts());
        }
        if (node.getQualifiedNames().size() > 1) {
            context.getRelBuilder().union(true, node.getQualifiedNames().size());
        }
        return null;
    }

    @Override
    public Void visitFilter(Filter node, CalcitePlanContext context) {
        visitChildren(node, context);
        RexNode condition = rexVisitor.analyze(node.getCondition(), context);
        context.getRelBuilder().filter(condition);
        return null;
    }

    @Override
    public Void visitProject(Project node, CalcitePlanContext context) {
        visitChildren(node, context);
        List<RexNode> projectList = node.getProjectList().stream()
            .filter(expr -> !(expr instanceof AllFields))
            .map(expr -> rexVisitor.analyze(expr, context))
            .collect(Collectors.toList());
        if (projectList.isEmpty()) {
            return null;
        }
        if (node.isExcluded()) {
            context.getRelBuilder().projectExcept(projectList);
        } else {
            context.getRelBuilder().project(projectList);
        }
        return null;
    }

    @Override
    public Void visitSort(Sort node, CalcitePlanContext context) {
        visitChildren(node, context);
        List<RexNode> sortList = node.getSortList().stream().map(
            expr -> {
                RexNode sortField = rexVisitor.analyze(expr, context);
                Sort.SortOption sortOption = analyzeSortOption(expr.getFieldArgs());
                if (sortOption == DEFAULT_DESC) {
                    return context.getRelBuilder().desc(sortField);
                } else {
                    return sortField;
                }
            }).collect(Collectors.toList());
        context.getRelBuilder().sort(sortList);
        return null;
    }

    private Sort.SortOption analyzeSortOption(List<Argument> fieldArgs) {
        Boolean asc = (Boolean) fieldArgs.get(0).getValue().getValue();
        Optional<Argument> nullFirst =
            fieldArgs.stream().filter(option -> "nullFirst".equals(option.getName())).findFirst();

        if (nullFirst.isPresent()) {
            Boolean isNullFirst = (Boolean) nullFirst.get().getValue().getValue();
            return new Sort.SortOption((asc ? ASC : DESC), (isNullFirst ? NULL_FIRST : NULL_LAST));
        }
        return asc ? Sort.SortOption.DEFAULT_ASC : DEFAULT_DESC;
    }

    @Override
    public Void visitHead(Head node, CalcitePlanContext context) {
        visitChildren(node, context);
        context.getRelBuilder().limit(node.getFrom(), node.getSize());
        return null;
    }

    @Override
    public Void visitEval(Eval node, CalcitePlanContext context) {
        visitChildren(node, context);
        List<String> originalFieldNames = context.getRelBuilder().peek().getRowType().getFieldNames();
        List<RexNode> evalList = node.getExpressionList().stream()
            .map(expr -> {
                RexNode eval = rexVisitor.analyze(expr, context);
                context.getRelBuilder().projectPlus(eval);
                return eval;
            }).collect(Collectors.toList());
        // Overriding the existing field if the alias has the same name with original field name. For example, eval field = 1
        List<String> overriding = evalList.stream().filter(expr -> expr.getKind() == AS)
            .map(expr -> ((RexLiteral) ((RexCall) expr).getOperands().get(1)).getValueAs(String.class))
            .collect(Collectors.toList());
        overriding.retainAll(originalFieldNames);
        if (!overriding.isEmpty()) {
            List<RexNode> toDrop = context.getRelBuilder().fields(overriding);
            context.getRelBuilder().projectExcept(toDrop);
        }
        return null;
    }

    @Override
    public Void visitAggregation(Aggregation node, CalcitePlanContext context) {
        visitChildren(node, context);
        List<AggCall> aggList = node.getAggExprList().stream()
            .map(expr -> aggVisitor.analyze(expr, context))
            .collect(Collectors.toList());
        List<RexNode> groupByList = node.getGroupExprList().stream()
            .map(expr -> rexVisitor.analyze(expr, context))
            .collect(Collectors.toList());

        UnresolvedExpression span = node.getSpan();
        if (!Objects.isNull(span)) {
            RexNode spanRex = rexVisitor.analyze(span, context);
            groupByList.add(spanRex);
            //add span's group alias field (most recent added expression)
        }
//        List<RexNode> aggList = node.getAggExprList().stream()
//            .map(expr -> rexVisitor.analyze(expr, context))
//            .collect(Collectors.toList());
//        relBuilder.aggregate(relBuilder.groupKey(groupByList),
//            aggList.stream().map(rex -> (MyAggregateCall) rex)
//                .map(MyAggregateCall::getCall).collect(Collectors.toList()));
        context.getRelBuilder().aggregate(context.getRelBuilder().groupKey(groupByList), aggList);
        return null;
    }

    @Override
    public Void visitJoin(Join node, CalcitePlanContext context) {
        List<UnresolvedPlan> children = node.getChildren();
        children.forEach(c -> analyze(c, context));
        RexNode joinCondition = node.getJoinCondition().map(c -> rexVisitor.analyzeJoinCondition(c, context))
            .orElse(context.getRelBuilder().literal(true));
        context.getRelBuilder().join(translateJoinType(node.getJoinType()), joinCondition);
        return null;
    }

    @Override
    public Void visitSubqueryAlias(SubqueryAlias node, CalcitePlanContext context) {
        visitChildren(node, context);
        context.getRelBuilder().as(node.getAlias());
        return null;
    }
}
