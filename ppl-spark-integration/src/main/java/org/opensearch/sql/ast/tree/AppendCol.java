package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

import java.util.List;

/**
 * A composite object which store the subQuery along with some more ad-hoc option like override
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
public class AppendCol extends UnresolvedPlan {

    public boolean override = true;

    private UnresolvedPlan subSearch;

    private UnresolvedPlan child;

    public AppendCol(UnresolvedPlan subSearch) {
        this.subSearch = subSearch;
    }

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }

    @Override
    public List<? extends Node> getChild() {
        return ImmutableList.of(child);
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> visitor, C context) {
        return visitor.visitAppendCol(this, context);
    }
}
