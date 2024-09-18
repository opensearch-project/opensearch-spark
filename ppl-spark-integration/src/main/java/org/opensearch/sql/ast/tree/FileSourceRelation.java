/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

import java.util.List; /**
 * AST node class for a sequence of files (such as CSV files) to read.
 */
public class FileSourceRelation extends UnresolvedPlan {
    private final List<String> paths;
    private final String format;
    private final String compressionCodeName;

    public FileSourceRelation(List<String> paths, String format) {
        this(paths, format, null);
    }

    public FileSourceRelation(List<String> paths, String format, String compressionCodeName) {
        this.paths = paths;
        this.format = format;
        this.compressionCodeName = compressionCodeName;
    }

    public List<String> getPaths() {
        return paths;
    }

    public String getFormat() {
        return format;
    }

    public String getCompressionCodeName() {
        return compressionCodeName;
    }

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        throw new UnsupportedOperationException("PathsReader node is supposed to have no child node");
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitFileSourceRelation(this, context);
    }

    @Override
    public List<? extends Node> getChild() {
        return ImmutableList.of();
    }
}
