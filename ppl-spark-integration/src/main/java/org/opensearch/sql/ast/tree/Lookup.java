/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Field;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** AST node represent Lookup operation. */
public class Lookup extends UnresolvedPlan {
    private UnresolvedPlan child;
    private final Relation lookupRelation;
    private final Map<Alias, Field> lookupMappingMap;
    private final OutputStrategy outputStrategy;
    private final Map<Alias, Field> outputCandidateMap;

    public Lookup(
        Relation lookupRelation,
        Map<Alias, Field> lookupMappingMap,
        OutputStrategy outputStrategy,
        Map<Alias, Field> outputCandidateMap) {
        this.lookupRelation = lookupRelation;
        this.lookupMappingMap = lookupMappingMap;
        this.outputStrategy = outputStrategy;
        this.outputCandidateMap = outputCandidateMap;
    }

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.child = new SubqueryAlias(child, "_s"); // add a auto generated alias name
//        this.child = child;
        return this;
    }

    @Override
    public List<? extends Node> getChild() {
        return ImmutableList.of(child);
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> visitor, C context) {
        return visitor.visitLookup(this, context);
    }

    public enum OutputStrategy {
        APPEND,
        REPLACE
    }

    public Relation getLookupRelation() {
        return lookupRelation;
    }

    /**
     * Lookup mapping field map. For example:
     *  1. When mapping is "name AS cName",
     *     the original key will be Alias(cName, Field(name)), the original value will be Field(cName).
     *     Returns a map which left join key is Field(name), right join key is Field(cName)
     *  2. When mapping is "dept",
     *     the original key is Alias(dept, Field(dept)), the original value is Field(dept).
     *     Returns a map which left join key is Field(dept), the right join key is Field(dept) too.
     */
    public Map<Field, Field> getLookupMappingMap() {
        return lookupMappingMap.entrySet().stream()
            .collect(Collectors.toMap(entry -> (Field) (entry.getKey()).getDelegated(), Map.Entry::getValue));
    }

    public OutputStrategy getOutputStrategy() {
        return outputStrategy;
    }

    /**
     * Output candidate field map. For example:
     *  1. When output candidate is "name AS cName", the key will be Alias(cName, Field(name)), the value will be Field(cName)
     *  2. When output candidate is "dept", the key is Alias(dept, Field(dept)), value is Field(dept)
     */
    public Map<Alias, Field> getOutputCandidateMap() {
        return outputCandidateMap;
    }

    /** Return the lookup output Field list instead of Alias list */
    public List<Field> getLookupOutputFieldList() {
        return outputCandidateMap.keySet().stream().map(alias -> (Field) alias.getDelegated()).collect(Collectors.toList());
    }

    public boolean allFieldsShouldAppliedToOutputList() {
        return getOutputCandidateMap().isEmpty();
    }

}
