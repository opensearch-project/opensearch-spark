package org.opensearch.sql.ast.tree;

import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FillNull extends UnresolvedPlan { ;

    public static class NullableFieldFill {
        private final Field nullableFieldReference;
        private final Literal replaceNullWithMe;

        public NullableFieldFill(Field nullableFieldReference, Literal replaceNullWithMe) {
            this.nullableFieldReference = Objects.requireNonNull(nullableFieldReference, "Field to replace is required");
            this.replaceNullWithMe = Objects.requireNonNull(replaceNullWithMe, "Null replacement is required");
        }

        public Field getNullableFieldReference() {
            return nullableFieldReference;
        }

        public Literal getReplaceNullWithMe() {
            return replaceNullWithMe;
        }
    }

    private interface ContainNullableFieldFill {
        Stream<NullableFieldFill> getNullFieldFill();
    }

    public static class SameValueNullFill implements ContainNullableFieldFill {
        private final List<NullableFieldFill> replacements;

        public SameValueNullFill(Literal replaceNullWithMe, List<Field> nullableFieldReferences) {
            Objects.requireNonNull(replaceNullWithMe, "Null replacement is required");
            this.replacements = Objects.requireNonNull(nullableFieldReferences, "Nullable field reference is required")
                    .stream()
                    .map(nullableReference -> new NullableFieldFill(nullableReference, replaceNullWithMe))
                    .collect(Collectors.toList());
        }

        @Override
        public Stream<NullableFieldFill> getNullFieldFill() {
            return replacements.stream();
        }
    }

    public static class VariousValueNullFill implements ContainNullableFieldFill {
        private final List<NullableFieldFill> replacements;

        public VariousValueNullFill(List<NullableFieldFill> replacements) {
            this.replacements = replacements;
        }

        @Override
        public Stream<NullableFieldFill> getNullFieldFill() {
            return replacements.stream();
        }
    }

    private UnresolvedPlan child;
    private final SameValueNullFill sameValueNullFill;
    private final VariousValueNullFill variousValueNullFill;

    public FillNull(SameValueNullFill sameValueNullFill, VariousValueNullFill variousValueNullFill) {
        this.sameValueNullFill = sameValueNullFill;
        this.variousValueNullFill = variousValueNullFill;
    }

    public List<NullableFieldFill> getNullableFieldFills() {
        return Stream.of(sameValueNullFill, variousValueNullFill)
                .filter(Objects::nonNull)
                .flatMap(ContainNullableFieldFill::getNullFieldFill)
                .collect(Collectors.toList());
    }

    @Override
    public UnresolvedPlan attach(UnresolvedPlan child) {
        this.child = child;
        return this;
    }

    @Override
    public List<? extends Node> getChild() {

        return child == null ? List.of() : List.of(child);
    }

    @Override
    public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
        return nodeVisitor.visitFillNull(this, context);
    }
}
