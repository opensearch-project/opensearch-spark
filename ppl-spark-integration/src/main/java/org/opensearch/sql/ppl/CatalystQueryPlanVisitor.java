/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute$;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar$;
import org.apache.spark.sql.catalyst.expressions.Ascending$;
import org.apache.spark.sql.catalyst.expressions.CaseWhen;
import org.apache.spark.sql.catalyst.expressions.Descending$;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.InSubquery$;
import org.apache.spark.sql.catalyst.expressions.ListQuery$;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.Predicate;
import org.apache.spark.sql.catalyst.expressions.SortDirection;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.plans.logical.*;
import org.apache.spark.sql.execution.ExplainMode;
import org.apache.spark.sql.execution.command.DescribeTableCommand;
import org.apache.spark.sql.execution.command.ExplainCommand;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.BinaryExpression;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.FieldsMapping;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.InSubquery;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.IsEmpty;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Correlation;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.DescribeRelation;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
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
import org.opensearch.sql.ast.tree.TopAggregation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.ppl.utils.AggregatorTranslator;
import org.opensearch.sql.ppl.utils.BuiltinFunctionTranslator;
import org.opensearch.sql.ppl.utils.ComparatorTransformer;
import org.opensearch.sql.ppl.utils.ParseStrategy;
import org.opensearch.sql.ppl.utils.SortUtils;
import scala.Option;
import scala.Option$;
import scala.Tuple2;
import scala.collection.IterableLike;
import scala.collection.Seq;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.List.of;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.EQUAL;
import static org.opensearch.sql.ppl.CatalystPlanContext.findRelation;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.translate;
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
import static org.opensearch.sql.ppl.utils.RelationUtils.resolveField;
import static org.opensearch.sql.ppl.utils.WindowSpecTransformer.window;

/**
 * Utility class to traverse PPL logical plan and translate it into catalyst logical plan
 */
public class CatalystQueryPlanVisitor extends AbstractNodeVisitor<LogicalPlan, CatalystPlanContext> {

    private final ExpressionAnalyzer expressionAnalyzer;

    public CatalystQueryPlanVisitor() {
        this.expressionAnalyzer = new ExpressionAnalyzer();
    }

    public LogicalPlan visit(Statement plan, CatalystPlanContext context) {
        return plan.accept(this, context);
    }

    public LogicalPlan visitSubSearch(UnresolvedPlan plan, CatalystPlanContext context) {
        return plan.accept(this, context);
    }

    /**
     * Handle Query Statement.
     */
    @Override
    public LogicalPlan visitQuery(Query node, CatalystPlanContext context) {
        return node.getPlan().accept(this, context);
    }

    @Override
    public LogicalPlan visitExplain(Explain node, CatalystPlanContext context) {
        node.getStatement().accept(this, context);
        return context.apply(p -> new ExplainCommand(p, ExplainMode.fromString(node.getExplainMode().name())));
    }

    @Override
    public LogicalPlan visitRelation(Relation node, CatalystPlanContext context) {
        if (node instanceof DescribeRelation) {
            TableIdentifier identifier;
            if (node.getTableQualifiedName().getParts().size() == 1) {
                identifier = new TableIdentifier(node.getTableQualifiedName().getParts().get(0));
            } else if (node.getTableQualifiedName().getParts().size() == 2) {
                identifier = new TableIdentifier(
                        node.getTableQualifiedName().getParts().get(1),
                        Option$.MODULE$.apply(node.getTableQualifiedName().getParts().get(0)));
            } else if (node.getTableQualifiedName().getParts().size() == 3) {
                identifier = new TableIdentifier(
                        node.getTableQualifiedName().getParts().get(2),
                        Option$.MODULE$.apply(node.getTableQualifiedName().getParts().get(0)),
                        Option$.MODULE$.apply(node.getTableQualifiedName().getParts().get(1)));
            } else {
                throw new IllegalArgumentException("Invalid table name: " + node.getTableQualifiedName()
                        + " Syntax: [ database_name. ] table_name");
            }
            return context.with(
                    new DescribeTableCommand(
                            identifier,
                            scala.collection.immutable.Map$.MODULE$.<String, String>empty(),
                            true,
                            DescribeRelation$.MODULE$.getOutputAttrs()));
        }
        //regular sql algebraic relations
        node.getTableName().forEach(t ->
                // Resolving the qualifiedName which is composed of a datasource.schema.table
                context.withRelation(new UnresolvedRelation(seq(of(t.split("\\."))), CaseInsensitiveStringMap.empty(), false))
        );
        return context.getPlan();
    }

    @Override
    public LogicalPlan visitFilter(Filter node, CatalystPlanContext context) {
        node.getChild().get(0).accept(this, context);
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
        node.getChild().get(0).accept(this, context);

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
    public LogicalPlan visitCorrelation(Correlation node, CatalystPlanContext context) {
        node.getChild().get(0).accept(this, context);
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
        node.getChild().get(0).accept(this, context);
        return context.apply(left -> {
            LogicalPlan right = node.getRight().accept(this, context);
            Optional<Expression> joinCondition = node.getJoinCondition().map(c -> visitExpression(c, context));
            context.retainAllNamedParseExpressions(p -> p);
            context.retainAllPlans(p -> p);
            return join(left, right, node.getJoinType(), joinCondition, node.getJoinHint());
        });
    }

    @Override
    public LogicalPlan visitSubqueryAlias(SubqueryAlias node, CatalystPlanContext context) {
        node.getChild().get(0).accept(this, context);
        return context.apply(p -> {
            var alias = org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias$.MODULE$.apply(node.getAlias(), p);
            context.withSubqueryAlias(alias);
            return alias;
        });

    }

    @Override
    public LogicalPlan visitAggregation(Aggregation node, CatalystPlanContext context) {
        node.getChild().get(0).accept(this, context);
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
        if ((node instanceof TopAggregation) && ((TopAggregation) node).getResults().isPresent()) {
            context.apply(p -> (LogicalPlan) Limit.apply(new org.apache.spark.sql.catalyst.expressions.Literal(
                    ((TopAggregation) node).getResults().get().getValue(), org.apache.spark.sql.types.DataTypes.IntegerType), p));
        }
        return logicalPlan;
    }

    private static LogicalPlan extractedAggregation(CatalystPlanContext context) {
        Seq<Expression> groupingExpression = context.retainAllGroupingNamedParseExpressions(p -> p);
        Seq<NamedExpression> aggregateExpressions = context.retainAllNamedParseExpressions(p -> (NamedExpression) p);
        return context.apply(p -> new Aggregate(groupingExpression, aggregateExpressions, p));
    }

    @Override
    public LogicalPlan visitAlias(Alias node, CatalystPlanContext context) {
        expressionAnalyzer.visitAlias(node, context);
        return context.getPlan();
    }

    @Override
    public LogicalPlan visitProject(Project node, CatalystPlanContext context) {
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
        LogicalPlan child = node.getChild().get(0).accept(this, context);
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
        node.getChild().get(0).accept(this, context);
        visitFieldList(node.getSortList(), context);
        Seq<SortOrder> sortElements = context.retainAllNamedParseExpressions(exp -> SortUtils.getSortDirection(node, (NamedExpression) exp));
        return context.apply(p -> (LogicalPlan) new org.apache.spark.sql.catalyst.plans.logical.Sort(sortElements, true, p));
    }

    @Override
    public LogicalPlan visitHead(Head node, CatalystPlanContext context) {
        node.getChild().get(0).accept(this, context);
        return context.apply(p -> (LogicalPlan) Limit.apply(new org.apache.spark.sql.catalyst.expressions.Literal(
                node.getSize(), DataTypes.IntegerType), p));
    }

    @Override
    public LogicalPlan visitFillNull(FillNull fillNull, CatalystPlanContext context) {
        fillNull.getChild().get(0).accept(this, context);
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
        LogicalPlan resultWithoutDuplicatedColumns = context.apply(logicalPlan -> DataFrameDropColumns$.MODULE$.apply(seq(toDrop), logicalPlan));
        return Objects.requireNonNull(resultWithoutDuplicatedColumns, "FillNull operation failed");
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
        LogicalPlan child = node.getChild().get(0).accept(this, context);
        Expression sourceField = visitExpression(node.getSourceField(), context);
        ParseMethod parseMethod = node.getParseMethod();
        java.util.Map<String, Literal> arguments = node.getArguments();
        String pattern = (String) node.getPattern().getValue();
        return ParseStrategy.visitParseCommand(node, sourceField, parseMethod, arguments, pattern, context);
    }

    @Override
    public LogicalPlan visitRename(Rename node, CatalystPlanContext context) {
        node.getChild().get(0).accept(this, context);
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
        LogicalPlan child = node.getChild().get(0).accept(this, context);
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
        child = context.apply(p -> new org.apache.spark.sql.catalyst.plans.logical.Project(projectExpressions, p));
        return child;
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
        node.getChild().get(0).accept(this, context);
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

    /**
     * Expression Analyzer.
     */
    public class ExpressionAnalyzer extends AbstractNodeVisitor<Expression, CatalystPlanContext> {

        public Expression analyze(UnresolvedExpression unresolved, CatalystPlanContext context) {
            return unresolved.accept(this, context);
        }

        @Override
        public Expression visitLiteral(Literal node, CatalystPlanContext context) {
            return context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.Literal(
                    translate(node.getValue(), node.getType()), translate(node.getType())));
        }

        /**
         * generic binary (And, Or, Xor , ...) arithmetic expression resolver
         *
         * @param node
         * @param transformer
         * @param context
         * @return
         */
        public Expression visitBinaryArithmetic(BinaryExpression node, BiFunction<Expression, Expression, Expression> transformer, CatalystPlanContext context) {
            node.getLeft().accept(this, context);
            Optional<Expression> left = context.popNamedParseExpressions();
            node.getRight().accept(this, context);
            Optional<Expression> right = context.popNamedParseExpressions();
            if (left.isPresent() && right.isPresent()) {
                return transformer.apply(left.get(), right.get());
            } else if (left.isPresent()) {
                return context.getNamedParseExpressions().push(left.get());
            } else if (right.isPresent()) {
                return context.getNamedParseExpressions().push(right.get());
            }
            return null;

        }

        @Override
        public Expression visitAnd(And node, CatalystPlanContext context) {
            return visitBinaryArithmetic(node,
                    (left, right) -> context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.And(left, right)), context);
        }

        @Override
        public Expression visitOr(Or node, CatalystPlanContext context) {
            return visitBinaryArithmetic(node,
                    (left, right) -> context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.Or(left, right)), context);
        }

        @Override
        public Expression visitXor(Xor node, CatalystPlanContext context) {
            return visitBinaryArithmetic(node,
                    (left, right) -> context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.BitwiseXor(left, right)), context);
        }

        @Override
        public Expression visitNot(Not node, CatalystPlanContext context) {
            node.getExpression().accept(this, context);
            Optional<Expression> arg = context.popNamedParseExpressions();
            return arg.map(expression -> context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.Not(expression))).orElse(null);
        }

        @Override
        public Expression visitSpan(Span node, CatalystPlanContext context) {
            node.getField().accept(this, context);
            Expression field = (Expression) context.popNamedParseExpressions().get();
            node.getValue().accept(this, context);
            Expression value = (Expression) context.popNamedParseExpressions().get();
            return context.getNamedParseExpressions().push(window(field, value, node.getUnit()));
        }

        @Override
        public Expression visitAggregateFunction(AggregateFunction node, CatalystPlanContext context) {
            node.getField().accept(this, context);
            Expression arg = (Expression) context.popNamedParseExpressions().get();
            Expression aggregator = AggregatorTranslator.aggregator(node, arg);
            return context.getNamedParseExpressions().push(aggregator);
        }

        @Override
        public Expression visitCompare(Compare node, CatalystPlanContext context) {
            analyze(node.getLeft(), context);
            Optional<Expression> left = context.popNamedParseExpressions();
            analyze(node.getRight(), context);
            Optional<Expression> right = context.popNamedParseExpressions();
            if (left.isPresent() && right.isPresent()) {
                Predicate comparator = ComparatorTransformer.comparator(node, left.get(), right.get());
                return context.getNamedParseExpressions().push((org.apache.spark.sql.catalyst.expressions.Expression) comparator);
            }
            return null;
        }

        @Override
        public Expression visitQualifiedName(QualifiedName node, CatalystPlanContext context) {
            List<UnresolvedRelation> relation = findRelation(context.traversalContext());
            if (!relation.isEmpty()) {
                Optional<QualifiedName> resolveField = resolveField(relation, node, context.getRelations());
                return resolveField.map(qualifiedName -> context.getNamedParseExpressions().push(UnresolvedAttribute$.MODULE$.apply(seq(qualifiedName.getParts()))))
                    .orElse(resolveQualifiedNameWithSubqueryAlias(node, context));
            }
            return context.getNamedParseExpressions().push(UnresolvedAttribute$.MODULE$.apply(seq(node.getParts())));
        }

        /**
         * Resolve the qualified name with subquery alias: <br/>
         * - subqueryAlias1.joinKey = subqueryAlias2.joinKey <br/>
         * - tableName1.joinKey = subqueryAlias2.joinKey <br/>
         * - subqueryAlias1.joinKey = tableName2.joinKey <br/>
         */
        private Expression resolveQualifiedNameWithSubqueryAlias(QualifiedName node, CatalystPlanContext context) {
            if (node.getPrefix().isPresent() &&
                context.traversalContext().peek() instanceof org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias) {
                if (context.getSubqueryAlias().stream().map(p -> (org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias) p)
                    .anyMatch(a -> a.alias().equalsIgnoreCase(node.getPrefix().get().toString()))) {
                    return context.getNamedParseExpressions().push(UnresolvedAttribute$.MODULE$.apply(seq(node.getParts())));
                } else if (context.getRelations().stream().map(p -> (UnresolvedRelation) p)
                    .anyMatch(a -> a.tableName().equalsIgnoreCase(node.getPrefix().get().toString()))) {
                    return context.getNamedParseExpressions().push(UnresolvedAttribute$.MODULE$.apply(seq(node.getParts())));
                }
            }
            return null;
        }

        @Override
        public Expression visitCorrelationMapping(FieldsMapping node, CatalystPlanContext context) {
            return node.getChild().stream().map(expression ->
                    visitCompare((Compare) expression, context)
            ).reduce(org.apache.spark.sql.catalyst.expressions.And::new).orElse(null);
        }

        @Override
        public Expression visitAllFields(AllFields node, CatalystPlanContext context) {
            // Case of aggregation step - no start projection can be added
            if (context.getNamedParseExpressions().isEmpty()) {
                // Create an UnresolvedStar for all-fields projection
                context.getNamedParseExpressions().push(UnresolvedStar$.MODULE$.apply(Option.<Seq<String>>empty()));
            }
            return context.getNamedParseExpressions().peek();
        }

        @Override
        public Expression visitAlias(Alias node, CatalystPlanContext context) {
            node.getDelegated().accept(this, context);
            Expression arg = context.popNamedParseExpressions().get();
            return context.getNamedParseExpressions().push(
                    org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(arg,
                            node.getAlias() != null ? node.getAlias() : node.getName(),
                            NamedExpression.newExprId(),
                            seq(new java.util.ArrayList<String>()),
                            Option.empty(),
                            seq(new java.util.ArrayList<String>())));
        }

        @Override
        public Expression visitEval(Eval node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : Eval");
        }

        @Override
        public Expression visitFunction(Function node, CatalystPlanContext context) {
            List<Expression> arguments =
                    node.getFuncArgs().stream()
                            .map(
                                    unresolvedExpression -> {
                                        var ret = analyze(unresolvedExpression, context);
                                        if (ret == null) {
                                            throw new UnsupportedOperationException(
                                                    String.format("Invalid use of expression %s", unresolvedExpression));
                                        } else {
                                            return context.popNamedParseExpressions().get();
                                        }
                                    })
                            .collect(Collectors.toList());
            Expression function = BuiltinFunctionTranslator.builtinFunction(node, arguments);
            return context.getNamedParseExpressions().push(function);
        }

        @Override
        public Expression visitIsEmpty(IsEmpty node, CatalystPlanContext context) {
            Stack<Expression> namedParseExpressions = new Stack<>();
            namedParseExpressions.addAll(context.getNamedParseExpressions());
            Expression expression = visitCase(node.getCaseValue(), context);
            namedParseExpressions.add(expression);
            context.setNamedParseExpressions(namedParseExpressions);
            return expression;
        }

        @Override
        public Expression visitFillNull(FillNull fillNull, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : FillNull");
        }

        @Override
        public Expression visitInterval(Interval node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : Interval");
        }

        @Override
        public Expression visitDedupe(Dedupe node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : Dedupe");
        }

        @Override
        public Expression visitIn(In node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : In");
        }

        @Override
        public Expression visitKmeans(Kmeans node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : Kmeans");
        }

        @Override
        public Expression visitCase(Case node, CatalystPlanContext context) {
            Stack<Expression> initialNameExpressions = new Stack<>();
            initialNameExpressions.addAll(context.getNamedParseExpressions());
            analyze(node.getElseClause(), context);
            Expression elseValue = context.getNamedParseExpressions().pop();
            List<Tuple2<Expression, Expression>> whens = new ArrayList<>();
            for (When when : node.getWhenClauses()) {
                if (node.getCaseValue() == null) {
                    whens.add(
                            new Tuple2<>(
                                    analyze(when.getCondition(), context),
                                    analyze(when.getResult(), context)
                            )
                    );
                } else {
                    // Merge case value and condition (compare value) into a single equal condition
                    Compare compare = new Compare(EQUAL.getName().getFunctionName(), node.getCaseValue(), when.getCondition());
                    whens.add(
                            new Tuple2<>(
                                    analyze(compare, context), analyze(when.getResult(), context)
                            )
                    );
                }
                context.retainAllNamedParseExpressions(e -> e);
            }
            context.setNamedParseExpressions(initialNameExpressions);
            return context.getNamedParseExpressions().push(new CaseWhen(seq(whens), Option.apply(elseValue)));
        }

        @Override
        public Expression visitRareTopN(RareTopN node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : RareTopN");
        }

        @Override
        public Expression visitWindowFunction(WindowFunction node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : WindowFunction");
        }

        @Override
        public Expression visitInSubquery(InSubquery node, CatalystPlanContext outerContext) {
            CatalystPlanContext innerContext = new CatalystPlanContext();
            visitExpressionList(node.getChild(), innerContext);
            Seq<Expression> values = innerContext.retainAllNamedParseExpressions(p -> p);
            UnresolvedPlan outerPlan = node.getQuery();
            LogicalPlan subSearch = CatalystQueryPlanVisitor.this.visitSubSearch(outerPlan, innerContext);
            Expression inSubQuery = InSubquery$.MODULE$.apply(
                values,
                ListQuery$.MODULE$.apply(
                    subSearch,
                    seq(new java.util.ArrayList<Expression>()),
                    NamedExpression.newExprId(),
                    -1,
                    seq(new java.util.ArrayList<Expression>()),
                    Option.empty()));
            return outerContext.getNamedParseExpressions().push(inSubQuery);
        }
    }
}
