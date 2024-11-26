/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OptionList;
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedTableSpec;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.Transform;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.AttributeList;
import org.opensearch.sql.ast.expression.FieldList;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.statement.ProjectStatement;
import org.opensearch.sql.ppl.CatalystPlanContext;
import scala.Option;
import scala.collection.mutable.Seq;

import java.util.Optional;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.map;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.option;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;

public interface ProjectionUtils {

    /**
     * build a CreateTableAsSelect operator base on the ProjectStatement node
     *         
     *    'CreateTableAsSelect [identity(age)], unresolvedtablespec(Some(parquet), optionlist(), None, None, None, false), false, false
     *           :- 'UnresolvedIdentifier [student_partition_bucket], false
     *                             - 'Project [*]
     *                                 - 'UnresolvedRelation [spark_catalog, default, flint_ppl_test], [], false
     * */
    static CreateTableAsSelect visitProject(LogicalPlan plan, ProjectStatement node, CatalystPlanContext context) {
        Optional<String> using = node.getUsing().map(Enum::name);
        Optional<UnresolvedExpression> options = node.getOptions();
        Optional<UnresolvedExpression> partitionColumns = node.getPartitionColumns();
        partitionColumns.map(Node::getChild);
        IdentityTransform transform = new IdentityTransform(new FieldReference(seq(partitionColumns.toString())));

        Optional<UnresolvedExpression> location = node.getLocation();
        UnresolvedIdentifier name = new UnresolvedIdentifier(seq(node.getTableQualifiedName().getParts()), false);
        UnresolvedTableSpec tableSpec = new UnresolvedTableSpec(map(emptyMap()), option(using), new OptionList(seq()), Option.empty(), Option.empty(), Option.empty(), false);
        Seq<Transform> partitioning = partitionColumns.isPresent() ?
             seq(((AttributeList) partitionColumns.get()).getAttrList().stream().map(f -> new IdentityTransform(new FieldReference(seq(f.toString())))).collect(toList())) : seq();
        return new CreateTableAsSelect(name, partitioning, plan, tableSpec, map(emptyMap()), !node.isOverride(), false);   
    }
}
