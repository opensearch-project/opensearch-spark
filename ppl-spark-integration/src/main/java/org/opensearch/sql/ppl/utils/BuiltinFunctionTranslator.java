/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

import java.util.List;
import java.util.Locale;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static scala.Option.empty;

public interface BuiltinFunctionTranslator {

    static Expression builtinFunction(org.opensearch.sql.ast.expression.Function function, List<Expression> args) {
        if (BuiltinFunctionName.of(function.getFuncName()).isEmpty()) {
            // TODO change it when UDF is supported
            // TODO should we support more functions which are not PPL builtin functions. E.g Spark builtin functions
            throw new UnsupportedOperationException(function.getFuncName() + " is not a builtin function of PPL");
        } else {
            String name = BuiltinFunctionName.of(function.getFuncName()).get().name().toLowerCase(Locale.ROOT);
            return new UnresolvedFunction(seq(name), seq(args), false, empty(),false);
        }
    }
}
