/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.io.Serializable;

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
}
