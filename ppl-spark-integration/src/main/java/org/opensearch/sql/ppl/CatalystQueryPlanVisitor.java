/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar$;
import org.apache.spark.sql.catalyst.expressions.Ascending$;
import org.apache.spark.sql.catalyst.expressions.Descending$;
import org.apache.spark.sql.catalyst.expressions.Explode;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GeneratorOuter;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.SortDirection;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.DataFrameDropColumns$;
import org.apache.spark.sql.catalyst.plans.logical.DescribeRelation$;
import org.apache.spark.sql.catalyst.plans.logical.Generate;
import org.apache.spark.sql.catalyst.plans.logical.Limit;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan$;
import org.apache.spark.sql.catalyst.plans.logical.Project$;
import org.apache.spark.sql.execution.ExplainMode;
import org.apache.spark.sql.execution.command.DescribeTableCommand;
import org.apache.spark.sql.execution.command.ExplainCommand;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.opensearch.flint.spark.FlattenGenerator;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Correlation;
import org.opensearch.sql.ast.tree.CountedAggregation;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.DescribeRelation;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FieldSummary;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Flatten;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Kmeans;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareAggregation;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.Window;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.ppl.utils.FieldSummaryTransformer;
import org.opensearch.sql.ppl.utils.ParseTransformer;
import org.opensearch.sql.ppl.utils.SortUtils;
import org.opensearch.sql.ppl.utils.TrendlineCatalystUtils;
import org.opensearch.sql.ppl.utils.WindowSpecTransformer;
import scala.None$;
import scala.Option;
import scala.collection.IterableLike;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.List.of;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static org.opensearch.sql.ppl.utils.DedupeTransformer.retainMultipleDuplicateEvents;
import static org.opensearch.sql.ppl.utils.DedupeTransformer.retainMultipleDuplicateEventsAndKeepEmpty;
import static org.opensearch.sql.ppl.utils.DedupeTransformer.retainOneDuplicateEvent;
import static org.opensearch.sql.ppl.utils.DedupeTransformer.retainOneDuplicateEventAndKeepEmpty;
import static org.opensearch.sql.ppl.utils.JoinSpecTransformer.join;
import static org.opensearch.sql.ppl.utils.LookupTransformer.buildFieldWithLookupSubqueryAlias;
import static org.opensearch.sql.ppl.utils.LookupTransformer.buildLookupMappingCondition;
import static org.opensearch.sql.ppl.utils.LookupTransformer.buildLookupRelationProjectList;
import static org.opensearch.sql.ppl.utils.LookupTransformer.buildOutputProjectList;
import static org.opensearch.sql.ppl.utils.LookupTransformer.buildProjectListFromFields;
import static org.opensearch.sql.ppl.utils.RelationUtils.getTableIdentifier;
import static scala.collection.JavaConverters.seqAsJavaList;

/**
 * Utility class to traverse PPL logical plan and translate it into catalyst logical plan
 */
public class CatalystQueryPlanVisitor extends AbstractNodeVisitor<LogicalPlan, CatalystPlanContext> {

    private final CatalystExpressionVisitor expressionAnalyzer;

    public CatalystQueryPlanVisitor() {
        this.expressionAnalyzer = new CatalystExpressionVisitor(this);
    }

    public LogicalPlan visit(Statement plan, CatalystPlanContext context) {
        return plan.accept(this, context);
    }
    
    /**
     * Handle Query Statement.
     */
    @Override
    public LogicalPlan visitQuery(Query node, CatalystPlanContext context) {
        return node.getPlan().accept(this, context);
    }

    public LogicalPlan visitFirstChild(Node node, CatalystPlanContext context) {
        return node.getChild().get(0).accept(this, context);
    }

    @Override
    public LogicalPlan visitExplain(Explain node, CatalystPlanContext context) {
        node.getStatement().accept(this, context);
        return context.apply(p -> new ExplainCommand(p, ExplainMode.fromString(node.getExplainMode().name())));
    }

    @Override
    public LogicalPlan visitRelation(Relation node, CatalystPlanContext context) {
        //relations doesnt have a visitFirstChild call since its the leaf of the AST tree 
        if (node instanceof DescribeRelation) {
            TableIdentifier identifier = getTableIdentifier(node.getTableQualifiedName());
            return context.with(
                    new DescribeTableCommand(
                            identifier,
                            scala.collection.immutable.Map$.MODULE$.<String, String>empty(),
                            true,
                            DescribeRelation$.MODULE$.getOutputAttrs()));
        }
        //regular sql algebraic relations
        node.getQualifiedNames().forEach(q ->
            // TODO Do not support 4+ parts table identifier in future (may be reverted this PR in 0.8.0)
            // node.getQualifiedNames.getParts().size() > 3
            // A Spark TableIdentifier should only contain 3 parts: tableName, databaseName and catalogName.
            // If the qualifiedName has more than 3 parts,
            // we merge all parts from 3 to last parts into the tableName as one whole
            context.withRelation(new UnresolvedRelation(seq(q.getParts()), CaseInsensitiveStringMap.empty(), false))
        );
        return context.getPlan();
    }

    @Override
    public LogicalPlan visitFilter(Filter node, CatalystPlanContext context) {
        visitFirstChild(node, context);
        return context.apply(p -> {
            Expression conditionExpression = visitExpression(node.getCondition(), context);
            Optional<Expression> innerConditionExpression = context.popNamedParseExpressions();
            return innerConditionExpression.map(expression -> new org.apache.spark.sql.catalyst.plans.logical.Filter(innerConditionExpression.get(), p)).orElse(null);
        });
    }

    /**
     * | LOOKUP <lookupIndex> (<lookupMappingField> [AS <sourceMappingField>])...
     *    [(REPLACE | APPEND) (<inputField> [AS <outputField])...]
     */
    @Override
    public LogicalPlan visitLookup(Lookup node, CatalystPlanContext context) {
        visitFirstChild(node, context);
        return context.apply( searchSide -> {
            LogicalPlan lookupTable = node.getLookupRelation().accept(this, context);
            Expression lookupCondition = buildLookupMappingCondition(node, expressionAnalyzer, context);
            // If no output field is specified, all fields from lookup table are applied to the output.
            if (node.allFieldsShouldAppliedToOutputList()) {
                context.retainAllNamedParseExpressions(p -> p);
                context.retainAllPlans(p -> p);
                return join(searchSide, lookupTable, Join.JoinType.LEFT, Optional.of(lookupCondition), new Join.JoinHint());
            }

            // If the output fields are specified, build a project list for lookup table.
            // The mapping fields of lookup table should be added in this project list, otherwise join will fail.
            // So the mapping fields of lookup table should be dropped after join.
            List<NamedExpression> lookupTableProjectList = buildLookupRelationProjectList(node, expressionAnalyzer, context);
            LogicalPlan lookupTableWithProject = Project$.MODULE$.apply(seq(lookupTableProjectList), lookupTable);

            LogicalPlan join = join(searchSide, lookupTableWithProject, Join.JoinType.LEFT, Optional.of(lookupCondition), new Join.JoinHint());

            // Add all outputFields by __auto_generated_subquery_name_s.*
            List<NamedExpression> outputFieldsWithNewAdded = new ArrayList<>();
            outputFieldsWithNewAdded.add(UnresolvedStar$.MODULE$.apply(Option.apply(seq(node.getSourceSubqueryAliasName()))));

            // Add new columns based on different strategies:
            // Append:  coalesce($outputField, $"inputField").as(outputFieldName)
            // Replace: $outputField.as(outputFieldName)
            outputFieldsWithNewAdded.addAll(buildOutputProjectList(node, node.getOutputStrategy(), expressionAnalyzer, context));

            org.apache.spark.sql.catalyst.plans.logical.Project outputWithNewAdded = Project$.MODULE$.apply(seq(outputFieldsWithNewAdded), join);

            // Drop the mapping fields of lookup table in result:
            // For example, in command "LOOKUP lookTbl Field1 AS Field2, Field3",
            // the Field1 and Field3 are projection fields and join keys which will be dropped in result.
            List<Field> mappingFieldsOfLookup = node.getLookupMappingMap().entrySet().stream()
                .map(kv -> kv.getKey().getField() == kv.getValue().getField() ? buildFieldWithLookupSubqueryAlias(node, kv.getKey()) : kv.getKey())
                .collect(Collectors.toList());
//            List<Field> mappingFieldsOfLookup = new ArrayList<>(node.getLookupMappingMap().keySet());
            List<Expression> dropListOfLookupMappingFields =
                buildProjectListFromFields(mappingFieldsOfLookup, expressionAnalyzer, context).stream()
                    .map(Expression.class::cast).collect(Collectors.toList());
            // Drop the $sourceOutputField if existing
            List<Expression> dropListOfSourceFields =
                visitExpressionList(new ArrayList<>(node.getFieldListWithSourceSubqueryAlias()), context);
            List<Expression> toDrop = new ArrayList<>(dropListOfLookupMappingFields);
            toDrop.addAll(dropListOfSourceFields);

            LogicalPlan outputWithDropped = DataFrameDropColumns$.MODULE$.apply(seq(toDrop), outputWithNewAdded);

            context.retainAllNamedParseExpressions(p -> p);
            context.retainAllPlans(p -> p);
            return outputWithDropped;
        });
    }

    @Override
    public LogicalPlan visitTrendline(Trendline node, CatalystPlanContext context) {
        visitFirstChild(node, context);
        node.getSortByField()
                .ifPresent(sortField -> {
                    Expression sortFieldExpression = visitExpression(sortField, context);
                    Seq<SortOrder> sortOrder = context
                            .retainAllNamedParseExpressions(exp -> SortUtils.sortOrder(sortFieldExpression, SortUtils.isSortedAscending(sortField)));
                    context.apply(p -> new org.apache.spark.sql.catalyst.plans.logical.Sort(sortOrder, true, p));
                });

        List<NamedExpression> trendlineProjectExpressions = new ArrayList<>();

        if (context.getNamedParseExpressions().isEmpty()) {
            // Create an UnresolvedStar for all-fields projection
            trendlineProjectExpressions.add(UnresolvedStar$.MODULE$.apply(Option.empty()));
        }

        trendlineProjectExpressions.addAll(TrendlineCatalystUtils.visitTrendlineComputations(expressionAnalyzer, node.getComputations(), node.getSortByField(), context));

        return context.apply(p -> new org.apache.spark.sql.catalyst.plans.logical.Project(seq(trendlineProjectExpressions), p));
    }

    @Override
    public LogicalPlan visitCorrelation(Correlation node, CatalystPlanContext context) {
        visitFirstChild(node, context);
        context.reduce((left, right) -> {
            visitFieldList(node.getFieldsList().stream().map(Field::new).collect(Collectors.toList()), context);
            Seq<Expression> fields = context.retainAllNamedParseExpressions(e -> e);
            if (!Objects.isNull(node.getScope())) {
                // scope - this is a time base expression that timeframes the join to a specific period : (Time-field-name, value, unit)
                expressionAnalyzer.visitSpan(node.getScope(), context);
                context.popNamedParseExpressions().get();
            }
            expressionAnalyzer.visitCorrelationMapping(node.getMappingListContext(), context);
            Seq<Expression> mapping = context.retainAllNamedParseExpressions(e -> e);
            return join(node.getCorrelationType(), fields, mapping, left, right);
        });
        return context.getPlan();
    }

    @Override
    public LogicalPlan visitJoin(Join node, CatalystPlanContext context) {
        visitFirstChild(node, context);
        return context.apply(left -> {
            LogicalPlan right = node.getRight().accept(this, context);
            Optional<Expression> joinCondition = node.getJoinCondition()
                .map(c -> expressionAnalyzer.analyzeJoinCondition(c, context));
            context.retainAllNamedParseExpressions(p -> p);
            context.retainAllPlans(p -> p);
            return join(left, right, node.getJoinType(), joinCondition, node.getJoinHint());
        });
    }

    @Override
    public LogicalPlan visitSubqueryAlias(SubqueryAlias node, CatalystPlanContext context) {
        visitFirstChild(node, context);
        return context.apply(p -> {
            var alias = org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias$.MODULE$.apply(node.getAlias(), p);
            context.withSubqueryAlias(alias);
            return alias;
        });

    }

    @Override
    public LogicalPlan visitAggregation(Aggregation node, CatalystPlanContext context) {
        visitFirstChild(node, context);
        List<Expression> aggsExpList = visitExpressionList(node.getAggExprList(), context);
        List<Expression> groupExpList = visitExpressionList(node.getGroupExprList(), context);
        if (!groupExpList.isEmpty()) {
            //add group by fields to context
            context.getGroupingParseExpressions().addAll(groupExpList);
        }

        UnresolvedExpression span = node.getSpan();
        if (!Objects.isNull(span)) {
            span.accept(this, context);
            //add span's group alias field (most recent added expression)
            context.getGroupingParseExpressions().add(context.getNamedParseExpressions().peek());
        }
        // build the aggregation logical step
        LogicalPlan logicalPlan = extractedAggregation(context);

        // set sort direction according to command type (`rare` is Asc, `top` is Desc, default to Asc)
        List<SortDirection> sortDirections = new ArrayList<>();
        sortDirections.add(node instanceof RareAggregation ? Ascending$.MODULE$ : Descending$.MODULE$);

        if (!node.getSortExprList().isEmpty()) {
            visitExpressionList(node.getSortExprList(), context);
            Seq<SortOrder> sortElements = context.retainAllNamedParseExpressions(exp ->
                    new SortOrder(exp,
                            sortDirections.get(0),
                            sortDirections.get(0).defaultNullOrdering(),
                            seq(new ArrayList<Expression>())));
            context.apply(p -> new org.apache.spark.sql.catalyst.plans.logical.Sort(sortElements, true, logicalPlan));
        }
        //visit TopAggregation results limit
        if ((node instanceof CountedAggregation) && ((CountedAggregation) node).getResults().isPresent()) {
            context.apply(p -> (LogicalPlan) Limit.apply(new org.apache.spark.sql.catalyst.expressions.Literal(
                    ((CountedAggregation) node).getResults().get().getValue(), org.apache.spark.sql.types.DataTypes.IntegerType), p));
        }
        return logicalPlan;
    }

    private static LogicalPlan extractedAggregation(CatalystPlanContext context) {
        Seq<Expression> groupingExpression = context.retainAllGroupingNamedParseExpressions(p -> p);
        Seq<NamedExpression> aggregateExpressions = context.retainAllNamedParseExpressions(p -> (NamedExpression) p);
        return context.apply(p -> new Aggregate(groupingExpression, aggregateExpressions, p));
    }

    @Override
    public LogicalPlan visitWindow(Window node, CatalystPlanContext context) {
        visitFirstChild(node, context);
        List<Expression> windowFunctionExpList = visitExpressionList(node.getWindowFunctionList(), context);
        Seq<Expression> windowFunctionExpressions = context.retainAllNamedParseExpressions(p -> p);
        List<Expression> partitionExpList = visitExpressionList(node.getPartExprList(), context);
        UnresolvedExpression span = node.getSpan();
        if (!Objects.isNull(span)) {
            visitExpression(span, context);
        }
        Seq<Expression> partitionSpec = context.retainAllNamedParseExpressions(p -> p);
        Seq<SortOrder> orderSpec = seq(new ArrayList<SortOrder>());
        Seq<NamedExpression> aggregatorFunctions = seq(
            seqAsJavaList(windowFunctionExpressions).stream()
                .map(w -> WindowSpecTransformer.buildAggregateWindowFunction(w, partitionSpec, orderSpec))
                .collect(Collectors.toList()));
        return context.apply(p ->
            new org.apache.spark.sql.catalyst.plans.logical.Window(
                aggregatorFunctions,
                partitionSpec,
                orderSpec,
                p));
    }

    @Override
    public LogicalPlan visitAlias(Alias node, CatalystPlanContext context) {
        expressionAnalyzer.visitAlias(node, context);
        return context.getPlan();
    }

    @Override
    public LogicalPlan visitProject(Project node, CatalystPlanContext context) {
        //update plan's context prior to visiting node children
        if (node.isExcluded()) {
            List<UnresolvedExpression> intersect = context.getProjectedFields().stream()
                    .filter(node.getProjectList()::contains)
                    .collect(Collectors.toList());
            if (!intersect.isEmpty()) {
                // Fields in parent projection, but they have be excluded in child. For example,
                // source=t | fields - A, B | fields A, B, C will throw "[Field A, Field B] can't be resolved"
                throw new SyntaxCheckException(intersect + " can't be resolved");
            }
        } else {
            context.withProjectedFields(node.getProjectList());
        }
        LogicalPlan child = visitFirstChild(node, context);
        visitExpressionList(node.getProjectList(), context);

        // Create a projection list from the existing expressions
        Seq<?> projectList = seq(context.getNamedParseExpressions());
        if (!projectList.isEmpty()) {
            if (node.isExcluded()) {
                Seq<Expression> dropList = context.retainAllNamedParseExpressions(p -> p);
                // build the DataFrameDropColumns plan with drop list
                child = context.apply(p -> new org.apache.spark.sql.catalyst.plans.logical.DataFrameDropColumns(dropList, p));
            } else {
                Seq<NamedExpression> projectExpressions = context.retainAllNamedParseExpressions(p -> (NamedExpression) p);
                // build the plan with the projection step
                child = context.apply(p -> new org.apache.spark.sql.catalyst.plans.logical.Project(projectExpressions, p));
            }
        }
        return child;
    }

    @Override
    public LogicalPlan visitSort(Sort node, CatalystPlanContext context) {
        visitFirstChild(node, context);
        visitFieldList(node.getSortList(), context);
        Seq<SortOrder> sortElements = context.retainAllNamedParseExpressions(exp -> SortUtils.getSortDirection(node, (NamedExpression) exp));
        return context.apply(p -> (LogicalPlan) new org.apache.spark.sql.catalyst.plans.logical.Sort(sortElements, true, p));
    }

    @Override
    public LogicalPlan visitHead(Head node, CatalystPlanContext context) {
        visitFirstChild(node, context);
        return context.apply(p -> (LogicalPlan) Limit.apply(new org.apache.spark.sql.catalyst.expressions.Literal(
                node.getSize(), DataTypes.IntegerType), p));
    }

    @Override
    public LogicalPlan visitFieldSummary(FieldSummary fieldSummary, CatalystPlanContext context) {
        visitFirstChild(fieldSummary, context);
        return FieldSummaryTransformer.translate(fieldSummary, context);
    }

    @Override
    public LogicalPlan visitFillNull(FillNull fillNull, CatalystPlanContext context) {
        visitFirstChild(fillNull, context);
        List<UnresolvedExpression> aliases = new ArrayList<>();
        for(FillNull.NullableFieldFill nullableFieldFill : fillNull.getNullableFieldFills()) {
            Field field = nullableFieldFill.getNullableFieldReference();
            UnresolvedExpression replaceNullWithMe = nullableFieldFill.getReplaceNullWithMe();
            Function coalesce = new Function("coalesce", of(field, replaceNullWithMe));
            String fieldName = field.getField().toString();
            Alias alias = new Alias(fieldName, coalesce);
            aliases.add(alias);
        }
        if (context.getNamedParseExpressions().isEmpty()) {
            // Create an UnresolvedStar for all-fields projection
            context.getNamedParseExpressions().push(UnresolvedStar$.MODULE$.apply(Option.<Seq<String>>empty()));
        }
        // ((Alias) expressionList.get(0)).child().children().head()
        List<Expression> toDrop = visitExpressionList(aliases, context).stream()
                .map(org.apache.spark.sql.catalyst.expressions.Alias.class::cast)
                .map(org.apache.spark.sql.catalyst.expressions.Alias::child) // coalesce
                .map(UnresolvedFunction.class::cast)// coalesce
                .map(UnresolvedFunction::children) // Seq of coalesce arguments
                .map(IterableLike::head) // first function argument which is source field
                .collect(Collectors.toList());
        Seq<NamedExpression> projectExpressions = context.retainAllNamedParseExpressions(p -> (NamedExpression) p);
        // build the plan with the projection step
        context.apply(p -> new org.apache.spark.sql.catalyst.plans.logical.Project(projectExpressions, p));
        LogicalPlan resultWithoutDuplicatedColumns = context.apply(dropOriginalColumns(p -> p.children().head(), toDrop));
        return Objects.requireNonNull(resultWithoutDuplicatedColumns, "FillNull operation failed");
    }

    /**
     * This method is used to generate DataFrameDropColumns operator for dropping duplicated columns
     * in the original plan. Then achieving similar effect like updating columns.
     *
     * PLAN_ID_TAG is a mechanism inner Spark that explicitly specify a plan to resolve the
     * UnresolvedAttributes. Set toDrop expressions' PLAN_ID_TAG to the same value as that of the
     * original plan, so Spark will resolve them correctly by that plan instead of the child.
    */
    private java.util.function.Function<LogicalPlan, LogicalPlan> dropOriginalColumns(
        java.util.function.Function<LogicalPlan, LogicalPlan> findOriginalPlan,
        List<Expression> toDrop) {
        return logicalPlan -> {
            LogicalPlan originalPlan = findOriginalPlan.apply(logicalPlan);
            long planId = logicalPlan.hashCode();
            originalPlan.setTagValue(LogicalPlan$.MODULE$.PLAN_ID_TAG(), planId);
            toDrop.forEach(e -> e.setTagValue(LogicalPlan$.MODULE$.PLAN_ID_TAG(), planId));
            return DataFrameDropColumns$.MODULE$.apply(seq(toDrop), logicalPlan);
        };
    }

    @Override
    public LogicalPlan visitFlatten(Flatten flatten, CatalystPlanContext context) {
        visitFirstChild(flatten, context);
        if (context.getNamedParseExpressions().isEmpty()) {
            // Create an UnresolvedStar for all-fields projection
            context.getNamedParseExpressions().push(UnresolvedStar$.MODULE$.apply(Option.<Seq<String>>empty()));
        }
        Expression field = visitExpression(flatten.getField(), context);
        List<Expression> alias = flatten.getAliasSequence().stream()
            .map(aliasNode -> visitExpression(aliasNode, context))
            .collect(Collectors.toList());
        context.retainAllNamedParseExpressions(p -> (NamedExpression) p);
        FlattenGenerator flattenGenerator = new FlattenGenerator(field);
        scala.collection.mutable.Seq outputs = alias.isEmpty() ? seq() : seq(alias);
        context.apply(p -> new Generate(new GeneratorOuter(flattenGenerator), seq(), true, (Option) None$.MODULE$, outputs, p));
        return context.apply(logicalPlan -> DataFrameDropColumns$.MODULE$.apply(seq(field), logicalPlan));
    }

    @Override
    public LogicalPlan visitExpand(org.opensearch.sql.ast.tree.Expand node, CatalystPlanContext context) {
        visitFirstChild(node, context);
        if (context.getNamedParseExpressions().isEmpty()) {
            // Create an UnresolvedStar for all-fields projection
            context.getNamedParseExpressions().push(UnresolvedStar$.MODULE$.apply(Option.<Seq<String>>empty()));
        }
        Expression field = visitExpression(node.getField(), context);
        Optional<Expression> alias = node.getAlias().map(aliasNode -> visitExpression(aliasNode, context));
        context.retainAllNamedParseExpressions(p -> (NamedExpression) p);
        Explode explodeGenerator = new Explode(field);
        scala.collection.mutable.Seq outputs = alias.isEmpty() ? seq() : seq(alias.get());
        if(alias.isEmpty())
            return context.apply(p -> new Generate(explodeGenerator, seq(), false, (Option) None$.MODULE$, outputs, p));
        else {
            //in case an alias does appear - remove the original field from the returning columns
            context.apply(p -> new Generate(explodeGenerator, seq(), false, (Option) None$.MODULE$, outputs, p));
            return context.apply(logicalPlan -> DataFrameDropColumns$.MODULE$.apply(seq(field), logicalPlan));
        }
    }

    private void visitFieldList(List<Field> fieldList, CatalystPlanContext context) {
        fieldList.forEach(field -> visitExpression(field, context));
    }

    private List<Expression> visitExpressionList(List<UnresolvedExpression> expressionList, CatalystPlanContext context) {
        return expressionList.isEmpty()
                ? emptyList()
                : expressionList.stream().map(field -> visitExpression(field, context))
                .collect(Collectors.toList());
    }

    private Expression visitExpression(UnresolvedExpression expression, CatalystPlanContext context) {
        return expressionAnalyzer.analyze(expression, context);
    }

    @Override
    public LogicalPlan visitParse(Parse node, CatalystPlanContext context) {
        visitFirstChild(node, context);
        Expression sourceField = visitExpression(node.getSourceField(), context);
        ParseMethod parseMethod = node.getParseMethod();
        java.util.Map<String, Literal> arguments = node.getArguments();
        String pattern = (String) node.getPattern().getValue();
        return ParseTransformer.visitParseCommand(node, sourceField, parseMethod, arguments, pattern, context);
    }

    @Override
    public LogicalPlan visitRename(Rename node, CatalystPlanContext context) {
        visitFirstChild(node, context);
        if (context.getNamedParseExpressions().isEmpty()) {
            // Create an UnresolvedStar for all-fields projection
            context.getNamedParseExpressions().push(UnresolvedStar$.MODULE$.apply(Option.empty()));
        }
        List<Expression> fieldsToRemove = visitExpressionList(node.getRenameList(), context).stream()
                .map(expression -> (org.apache.spark.sql.catalyst.expressions.Alias) expression)
                .map(org.apache.spark.sql.catalyst.expressions.Alias::child)
                .collect(Collectors.toList());
        Seq<NamedExpression> projectExpressions = context.retainAllNamedParseExpressions(p -> (NamedExpression) p);
        // build the plan with the projection step
        LogicalPlan outputWithSourceColumns = context.apply(p -> new org.apache.spark.sql.catalyst.plans.logical.Project(projectExpressions, p));
        return context.apply(p -> DataFrameDropColumns$.MODULE$.apply(seq(fieldsToRemove), outputWithSourceColumns));
    }

    @Override
    public LogicalPlan visitEval(Eval node, CatalystPlanContext context) {
        visitFirstChild(node, context);
        List<UnresolvedExpression> aliases = new ArrayList<>();
        List<Let> letExpressions = node.getExpressionList();
        for (Let let : letExpressions) {
            Alias alias = new Alias(let.getVar().getField().toString(), let.getExpression());
            aliases.add(alias);
        }
        if (context.getNamedParseExpressions().isEmpty()) {
            // Create an UnresolvedStar for all-fields projection
            context.getNamedParseExpressions().push(UnresolvedStar$.MODULE$.apply(Option.<Seq<String>>empty()));
        }
        List<Expression> expressionList = visitExpressionList(aliases, context);
        Seq<NamedExpression> projectExpressions = context.retainAllNamedParseExpressions(p -> (NamedExpression) p);
        // build the plan with the projection step
        return context.apply(p -> new org.apache.spark.sql.catalyst.plans.logical.Project(projectExpressions, p));
    }

    @Override
    public LogicalPlan visitKmeans(Kmeans node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : Kmeans");
    }

    @Override
    public LogicalPlan visitIn(In node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : In");
    }

    @Override
    public LogicalPlan visitRareTopN(RareTopN node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : RareTopN");
    }

    @Override
    public LogicalPlan visitWindowFunction(WindowFunction node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : WindowFunction");
    }

    @Override
    public LogicalPlan visitDedupe(Dedupe node, CatalystPlanContext context) {
        visitFirstChild(node, context);
        List<Argument> options = node.getOptions();
        Integer allowedDuplication = (Integer) options.get(0).getValue().getValue();
        Boolean keepEmpty = (Boolean) options.get(1).getValue().getValue();
        Boolean consecutive = (Boolean) options.get(2).getValue().getValue();
        if (allowedDuplication <= 0) {
            throw new IllegalArgumentException("Number of duplicate events must be greater than 0");
        }
        if (consecutive) {
            // Spark is not able to remove only consecutive events
            throw new UnsupportedOperationException("Consecutive deduplication is not supported");
        }
        visitFieldList(node.getFields(), context);
        // Columns to deduplicate
        Seq<org.apache.spark.sql.catalyst.expressions.Attribute> dedupeFields
                = context.retainAllNamedParseExpressions(e -> (org.apache.spark.sql.catalyst.expressions.Attribute) e);
        // Although we can also use the Window operator to translate this as allowedDuplication > 1 did,
        // adding Aggregate operator could achieve better performance.
        if (allowedDuplication == 1) {
            if (keepEmpty) {
                return retainOneDuplicateEventAndKeepEmpty(node, dedupeFields, expressionAnalyzer, context);
            } else {
                return retainOneDuplicateEvent(node, dedupeFields, expressionAnalyzer, context);
            }
        } else {
            if (keepEmpty) {
                return retainMultipleDuplicateEventsAndKeepEmpty(node, allowedDuplication, expressionAnalyzer, context);
            } else {
                return retainMultipleDuplicateEvents(node, allowedDuplication, expressionAnalyzer, context);
            }
        }
    }
}
