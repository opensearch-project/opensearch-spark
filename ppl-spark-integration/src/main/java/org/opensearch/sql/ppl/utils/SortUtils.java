/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.expressions.Ascending$;
import org.apache.spark.sql.catalyst.expressions.Descending$;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.tree.Sort;

import java.util.ArrayList;
import java.util.Optional;

import static scala.collection.JavaConverters.asScalaBufferConverter;

/**
 * Utility interface for sorting operations.
 * Provides methods to generate sort orders based on given criteria.
 */
public interface SortUtils {

    /**
     * Retrieves the sort direction for a given field name from a sort node.
     *
     * @param node      The sort node containing the list of fields and their sort directions.
     * @param expression The field name for which the sort direction is to be retrieved.
     * @return SortOrder representing the sort direction of the given field name or null if the field is not found.
     */
    static SortOrder getSortDirection(Sort node, NamedExpression expression) {
        Optional<Field> field = node.getSortList().stream()
                .filter(f -> f.getField().toString().equals(expression.name()))
                .findAny();

        if(field.isPresent()) {
            return new SortOrder(
                    (Expression) expression,
                    (Boolean)field.get().getFieldArgs().get(0).getValue().getValue() ? Ascending$.MODULE$ : Descending$.MODULE$,
                    (Boolean)field.get().getFieldArgs().get(0).getValue().getValue() ? Ascending$.MODULE$.defaultNullOrdering() : Descending$.MODULE$.defaultNullOrdering(),
                    asScalaBufferConverter(new ArrayList<Expression>()).asScala().seq()
            );
        }
        return null;
    }
}