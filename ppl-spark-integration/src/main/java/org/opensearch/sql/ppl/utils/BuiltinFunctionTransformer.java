/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction$;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADD;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADDDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DATEDIFF;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DAY_OF_MONTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.COALESCE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_ARRAY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_ARRAY_LENGTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_EXTRACT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_KEYS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_OBJECT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_VALID;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBTRACT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MULTIPLY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DIVIDE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MODULUS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DAY_OF_WEEK;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DAY_OF_YEAR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.HOUR_OF_DAY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NOT_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LENGTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LOCALTIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MINUTE_OF_HOUR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MONTH_OF_YEAR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SECOND_OF_MINUTE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SYSDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TRIM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WEEK;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WEEK_OF_YEAR;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static scala.Option.empty;

public interface BuiltinFunctionTransformer {

    /**
     * The name mapping between PPL builtin functions to Spark builtin functions.
     * This is only used for the built-in functions between PPL and Spark with different names.
     * If the built-in function names are the same in PPL and Spark, add it to {@link BuiltinFunctionName} only.
     */
    static final Map<BuiltinFunctionName, String> SPARK_BUILTIN_FUNCTION_NAME_MAPPING
        = ImmutableMap.<BuiltinFunctionName, String>builder()
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
            .put(SYSDATE, "now")
            // condition functions
            .put(IS_NULL, "isnull")
            .put(IS_NOT_NULL, "isnotnull")
            .put(BuiltinFunctionName.ISPRESENT, "isnotnull")
            .put(COALESCE, "coalesce")
            .put(LENGTH, "length")
            .put(TRIM, "trim")
            // json functions
            .put(JSON_KEYS, "json_object_keys")
            .put(JSON_EXTRACT, "get_json_object")
            .build();

    /**
     * The name mapping between PPL builtin functions to Spark builtin functions.
     */
    static final Map<BuiltinFunctionName, Function<List<Expression>, UnresolvedFunction>> PPL_TO_SPARK_FUNC_MAPPING
        = ImmutableMap.<BuiltinFunctionName, Function<List<Expression>, UnresolvedFunction>>builder()
        // json functions
        .put(
            JSON_ARRAY,
            args -> {
                return UnresolvedFunction$.MODULE$.apply("array", seq(args), false);
            })
        .put(
            JSON_OBJECT,
            args -> {
                return UnresolvedFunction$.MODULE$.apply("named_struct", seq(args), false);
            })
        .put(
            JSON_ARRAY_LENGTH,
            args -> {
                // Check if the input is an array (from json_array()) or a JSON string
                if (args.get(0) instanceof UnresolvedFunction) {
                    // Input is a JSON array
                    return UnresolvedFunction$.MODULE$.apply("json_array_length",
                        seq(UnresolvedFunction$.MODULE$.apply("to_json", seq(args), false)), false);
                } else {
                    // Input is a JSON string
                    return UnresolvedFunction$.MODULE$.apply("json_array_length", seq(args.get(0)), false);
                }
            })
        .put(
            JSON,
            args -> {
                // Check if the input is a named_struct (from json_object()) or a JSON string
                if (args.get(0) instanceof UnresolvedFunction) {
                    return UnresolvedFunction$.MODULE$.apply("to_json", seq(args.get(0)), false);
                } else {
                    return UnresolvedFunction$.MODULE$.apply("get_json_object",
                        seq(args.get(0), Literal$.MODULE$.apply("$")), false);
                }
            })
        .put(
            JSON_VALID,
            args -> {
                return UnresolvedFunction$.MODULE$.apply("isnotnull",
                    seq(UnresolvedFunction$.MODULE$.apply("get_json_object",
                        seq(args.get(0), Literal$.MODULE$.apply("$")), false)), false);
            })
        .build();

    static Expression builtinFunction(org.opensearch.sql.ast.expression.Function function, List<Expression> args) {
        if (BuiltinFunctionName.of(function.getFuncName()).isEmpty()) {
            // TODO change it when UDF is supported
            // TODO should we support more functions which are not PPL builtin functions. E.g Spark builtin functions
            throw new UnsupportedOperationException(function.getFuncName() + " is not a builtin function of PPL");
        } else {
            BuiltinFunctionName builtin = BuiltinFunctionName.of(function.getFuncName()).get();
            String name = SPARK_BUILTIN_FUNCTION_NAME_MAPPING.get(builtin);
            if (name != null) {
                // there is a Spark builtin function mapping with the PPL builtin function
                return new UnresolvedFunction(seq(name), seq(args), false, empty(),false);
            }
            Function<List<Expression>, UnresolvedFunction> alternative = PPL_TO_SPARK_FUNC_MAPPING.get(builtin);
            if (alternative != null) {
                return alternative.apply(args);
            }
            name = builtin.getName().getFunctionName();
            return new UnresolvedFunction(seq(name), seq(args), false, empty(),false);
        }
    }
}
