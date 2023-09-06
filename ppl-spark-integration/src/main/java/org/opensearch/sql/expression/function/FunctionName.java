/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.io.Serializable;
import java.util.Objects;

/**
 * The definition of Function Name.
 */
public class FunctionName implements Serializable {
    private  String functionName;

    public FunctionName(String functionName) {
        this.functionName = functionName;
    }

    public static FunctionName of(String functionName) {
        return new FunctionName(functionName.toLowerCase());
    }

    @Override
    public String toString() {
        return functionName;
    }

    public String getFunctionName() {
        return toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FunctionName that = (FunctionName) o;
        return Objects.equals(getFunctionName(), that.getFunctionName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFunctionName());
    }
}
