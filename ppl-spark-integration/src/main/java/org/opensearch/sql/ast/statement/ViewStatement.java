/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.ast.statement;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.DataSourceType;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ppl.utils.TableIdentifier;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Project Statement.
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class ViewStatement extends Statement implements TableIdentifier {

    private final Statement statement;
    private final boolean override;
    private final List<UnresolvedExpression> tableNames;

    private final Optional<DataSourceType> using;
    private final Optional<UnresolvedExpression> options;
    private final Optional<UnresolvedExpression> partitionColumns;
    private final Optional<UnresolvedExpression> location;

    public ViewStatement(List<UnresolvedExpression> tableNames, Optional<UnresolvedExpression> using,
                         Optional<UnresolvedExpression> options, Optional<UnresolvedExpression> partitionColumns,
                         Optional<UnresolvedExpression> location, Query statement, boolean override) {
        this.tableNames = tableNames;
        this.using = using.map(p->DataSourceType.valueOf(p.toString()));
        this.options = options;
        this.partitionColumns = partitionColumns;
        this.location = location;
        this.statement = statement;
        this.override = override;
    }

    public List<QualifiedName> getQualifiedNames() {
        return tableNames.stream().map(t -> (QualifiedName) t).collect(Collectors.toList());
    }

    /**
     * Get Qualified name preservs parts of the user given identifiers. This can later be utilized to
     * determine DataSource,Schema and Table Name during Analyzer stage. So Passing QualifiedName
     * directly to Analyzer Stage.
     *
     * @return TableQualifiedName.
     */
    public QualifiedName getTableQualifiedName() {
        return getTableQualifiedName(this.tableNames);
    }

    @Override
    public <R, C> R accept(AbstractNodeVisitor<R, C> visitor, C context) {
        return visitor.visitViewStatement(this, context);
    }

}
