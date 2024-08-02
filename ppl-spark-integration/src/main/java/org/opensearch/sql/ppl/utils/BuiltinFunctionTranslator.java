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
import java.util.Map;

import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADD;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBTRACT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MULTIPLY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DIVIDE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MODULUS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DAY_OF_WEEK;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DAY_OF_MONTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DAY_OF_YEAR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WEEK_OF_YEAR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WEEK;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MONTH_OF_YEAR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.HOUR_OF_DAY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MINUTE_OF_HOUR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SECOND_OF_MINUTE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADDDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DATEDIFF;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LOCALTIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NOT_NULL;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static scala.Option.empty;

public interface BuiltinFunctionTranslator {

    /**
     * The name mapping between PPL builtin functions to Spark builtin functions.
     */
    static final Map<BuiltinFunctionName, String> SPARK_BUILTIN_FUNCTION_NAME_MAPPING
        = new ImmutableMap.Builder<BuiltinFunctionName, String>()
            // arithmetic operators
            .put(ADD, "+")
            .put(SUBTRACT, "-")
            .put(MULTIPLY, "*")
            .put(DIVIDE, "/")
            .put(MODULUS, "%")
            // time functions
            .put(DAY_OF_WEEK, "dayofweek")
            .put(DAY_OF_MONTH, "dayofmonth")
            .put(DAY_OF_YEAR, "dayofyear")
            .put(WEEK_OF_YEAR, "weekofyear")
            .put(WEEK, "weekofyear")
            .put(MONTH_OF_YEAR, "month")
            .put(HOUR_OF_DAY, "hour")
            .put(MINUTE_OF_HOUR, "minute")
            .put(SECOND_OF_MINUTE, "second")
            .put(SUBDATE, "date_sub") // only maps subdate(date, days)
            .put(ADDDATE, "date_add") // only maps adddate(date, days)
            .put(DATEDIFF, "datediff")
            .put(LOCALTIME, "localtimestamp")
            //condition functions
            .put(IS_NULL, "isnull")
            .put(IS_NOT_NULL, "isnotnull")
            .build();

    static Expression builtinFunction(org.opensearch.sql.ast.expression.Function function, List<Expression> args) {
        if (BuiltinFunctionName.of(function.getFuncName()).isEmpty()) {
            // TODO change it when UDF is supported
            // TODO should we support more functions which are not PPL builtin functions. E.g Spark builtin functions
            throw new UnsupportedOperationException(function.getFuncName() + " is not a builtin function of PPL");
        } else {
            BuiltinFunctionName builtin = BuiltinFunctionName.of(function.getFuncName()).get();
            String name = SPARK_BUILTIN_FUNCTION_NAME_MAPPING
                .getOrDefault(builtin, builtin.getName().getFunctionName());
            return new UnresolvedFunction(seq(name), seq(args), false, empty(),false);
        }
    }
}
