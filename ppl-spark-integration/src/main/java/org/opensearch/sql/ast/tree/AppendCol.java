package org.opensearch.sql.ast.tree;

import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;

/**
 * A composite object which store the subQuery along with some more ad-hoc option like override
 */
public class AppendCol extends UnresolvedPlan {


    public boolean override = true;

    private UnresolvedPlan child;


    public AppendCol(UnresolvedPlan child) {
        this.child = child;
    }

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> visitor, C context) {
        return visitor.visitAppendCol(this, context);
    }
}
