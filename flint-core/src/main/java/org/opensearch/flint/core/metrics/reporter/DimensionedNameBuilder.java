package org.opensearch.flint.core.metrics.reporter;

import com.amazonaws.services.cloudwatch.model.Dimension;
import java.util.HashMap;
import java.util.Map;

public class DimensionedNameBuilder {
    private final String name;
    private Map<String, Dimension> dimensions;

    DimensionedNameBuilder(final String name) {
        this(name, new HashMap<>());
    }

    DimensionedNameBuilder(final String name, final Map<String, Dimension> dimensions) {
        this.name = name;
        this.dimensions = dimensions;
    }

    public DimensionedName build() {
        return new DimensionedName(this.name, this.dimensions);
    }

    public DimensionedNameBuilder withDimension(final String name, final String value) {
        this.dimensions.put(name, new Dimension().withName(name).withValue(value));
        return this;
    }
}
