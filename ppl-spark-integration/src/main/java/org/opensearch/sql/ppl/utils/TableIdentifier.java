/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.ppl.utils;

import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.List;
import java.util.stream.Collectors;

public interface TableIdentifier {

    String COMMA = ",";

    /**
     * Get Qualified name preservs parts of the user given identifiers. This can later be utilized to
     * determine DataSource,Schema and Table Name during Analyzer stage. So Passing QualifiedName
     * directly to Analyzer Stage.
     *
     * @return TableQualifiedName.
     */
    default QualifiedName getTableQualifiedName(List<UnresolvedExpression> tableNames) {
        if (tableNames.size() == 1) {
            return (QualifiedName) tableNames.get(0);
        } else {
            return new QualifiedName(
                    tableNames.stream()
                            .map(UnresolvedExpression::toString)
                            .collect(Collectors.joining(COMMA)));
        }
    }
}
