/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static scala.Option.empty;

public interface BuiltinFunctionTranslator {

    /**
     * The name mapping between PPL builtin functions to Spark builtin functions.
     */
    static final Map<String, String> SPARK_BUILTIN_FUNCTION_NAME_MAPPING = new ImmutableMap.Builder<String, String>()
        // arithmetic operators
        .put(BuiltinFunctionName.ADD.name().toLowerCase(Locale.ROOT), "+")
        .put(BuiltinFunctionName.SUBTRACT.name().toLowerCase(Locale.ROOT), "-")
        .put(BuiltinFunctionName.MULTIPLY.name().toLowerCase(Locale.ROOT), "*")
        .put(BuiltinFunctionName.DIVIDE.name().toLowerCase(Locale.ROOT), "/")
        .put(BuiltinFunctionName.MODULUS.name().toLowerCase(Locale.ROOT), "%")
        // time functions
        .put(BuiltinFunctionName.DAY_OF_WEEK.name().toLowerCase(Locale.ROOT), "dayofweek")
        .put(BuiltinFunctionName.DAY_OF_MONTH.name().toLowerCase(Locale.ROOT), "dayofmonth")
        .put(BuiltinFunctionName.DAY_OF_YEAR.name().toLowerCase(Locale.ROOT), "dayofyear")
        .put(BuiltinFunctionName.WEEK_OF_YEAR.name().toLowerCase(Locale.ROOT), "weekofyear")
        .put(BuiltinFunctionName.WEEK.name().toLowerCase(Locale.ROOT), "weekofyear")
        .put(BuiltinFunctionName.MONTH_OF_YEAR.name().toLowerCase(Locale.ROOT), "month")
        .put(BuiltinFunctionName.HOUR_OF_DAY.name().toLowerCase(Locale.ROOT), "hour")
        .put(BuiltinFunctionName.MINUTE_OF_HOUR.name().toLowerCase(Locale.ROOT), "minute")
        .put(BuiltinFunctionName.SECOND_OF_MINUTE.name().toLowerCase(Locale.ROOT), "second")
        .put(BuiltinFunctionName.SUBDATE.name().toLowerCase(Locale.ROOT), "date_sub") // only maps subdate(date, days)
        .put(BuiltinFunctionName.ADDDATE.name().toLowerCase(Locale.ROOT), "date_add") // only maps adddate(date, days)
        .put(BuiltinFunctionName.DATEDIFF.name().toLowerCase(Locale.ROOT), "datediff")
        .put(BuiltinFunctionName.LOCALTIME.name().toLowerCase(Locale.ROOT), "localtimestamp")
        // condition functions
        .put(BuiltinFunctionName.IS_NULL.name().toLowerCase(Locale.ROOT), "isnull")
        .put(BuiltinFunctionName.IS_NOT_NULL.name().toLowerCase(Locale.ROOT), "isnotnull")
        .build();

    static Expression builtinFunction(org.opensearch.sql.ast.expression.Function function, List<Expression> args) {
        if (BuiltinFunctionName.of(function.getFuncName()).isEmpty()) {
            // TODO change it when UDF is supported
            // TODO should we support more functions which are not PPL builtin functions. E.g Spark builtin functions
            throw new UnsupportedOperationException(function.getFuncName() + " is not a builtin function of PPL");
        } else {
            String name = BuiltinFunctionName.of(function.getFuncName()).get().name().toLowerCase(Locale.ROOT);
            name = SPARK_BUILTIN_FUNCTION_NAME_MAPPING.getOrDefault(name, name);
            return new UnresolvedFunction(seq(name), seq(args), false, empty(),false);
        }
    }
}
