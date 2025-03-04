/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction$;
import org.apache.spark.sql.catalyst.expressions.CurrentTimeZone$;
import org.apache.spark.sql.catalyst.expressions.CurrentTimestamp$;
import org.apache.spark.sql.catalyst.expressions.DateAddInterval$;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual$;
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual$;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.catalyst.expressions.TimestampAdd$;
import org.apache.spark.sql.catalyst.expressions.TimestampDiff$;
import org.apache.spark.sql.catalyst.expressions.ToUTCTimestamp$;
import org.apache.spark.sql.catalyst.expressions.UnaryMinus$;
import org.opensearch.sql.ast.expression.IntervalUnit;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.SerializableUdf;
import scala.Option;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADD;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ADDDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.APPROX_COUNT_DISTINCT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.ARRAY_LENGTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CIDR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DATEDIFF;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DATE_ADD;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DATE_SUB;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.DAY_OF_MONTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.COALESCE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.EARLIEST;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IP_TO_INT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_IPV4;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_APPEND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_ARRAY;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_ARRAY_LENGTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_DELETE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_EXTEND;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_EXTRACT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_KEYS;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_OBJECT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_SET;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.JSON_VALID;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LATEST;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.RELATIVE_TIMESTAMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SECOND_OF_MINUTE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SUBDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.SYSDATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TIMESTAMPADD;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TIMESTAMPDIFF;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TO_JSON_STRING;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TRIM;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.UTC_TIMESTAMP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WEEK;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.WEEK_OF_YEAR;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static scala.Option.empty;

/**
 * Transformer for built-in functions. It will transform PPL built-in functions to Spark functions.
 * There are four ways of transformation:
 * 1. Transform to spark built-int function directly without name mapping
 * 2. Transform to spark built-int function with direct name mapping in `SPARK_BUILTIN_FUNCTION_NAME_MAPPING`
 * 3. Transform to spark built-int function with alternative implementation mapping in `PPL_TO_SPARK_FUNC_MAPPING`
 * 4. Transform to spark scala function implemented in {@link SerializableUdf} by mapping in `PPL_TO_SPARK_UDF_MAPPING`
 */
public interface BuiltinFunctionTransformer {

    /**
     * The name mapping between PPL builtin functions to Spark builtin functions.
     * This is only used for the built-in functions between PPL and Spark with different names.
     * If the built-in function names are the same in PPL and Spark, add it to {@link BuiltinFunctionName} only.
     */
    Map<BuiltinFunctionName, String> SPARK_BUILTIN_FUNCTION_NAME_MAPPING
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
            .put(ARRAY_LENGTH, "array_size")
            // json functions
            .put(TO_JSON_STRING, "to_json")
            .put(JSON_KEYS, "json_object_keys")
            .put(JSON_EXTRACT, "get_json_object")
            .put(APPROX_COUNT_DISTINCT, "approx_count_distinct")
            .build();

    /**
     * The name mapping between PPL builtin functions to Spark builtin functions.
     */
    Map<BuiltinFunctionName, Function<List<Expression>, Expression>> PPL_TO_SPARK_FUNC_MAPPING
        = ImmutableMap.<BuiltinFunctionName, Function<List<Expression>, Expression>>builder()
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
                return UnresolvedFunction$.MODULE$.apply("json_array_length", seq(args.get(0)), false);
            })
        .put(
            JSON,
            args -> {
                return UnresolvedFunction$.MODULE$.apply("get_json_object", seq(args.get(0), Literal$.MODULE$.apply("$")), false);
            })
        .put(
            JSON_VALID,
            args -> {
                return UnresolvedFunction$.MODULE$.apply("isnotnull",
                    seq(UnresolvedFunction$.MODULE$.apply("get_json_object",
                        seq(args.get(0), Literal$.MODULE$.apply("$")), false)), false);
            })
        .put(
            DATE_ADD,
            args -> {
                return DateAddInterval$.MODULE$.apply(args.get(0), args.get(1), Option.empty(), false);
            })
        .put(
            DATE_SUB,
            args -> {
                return DateAddInterval$.MODULE$.apply(args.get(0), UnaryMinus$.MODULE$.apply(args.get(1), true), Option.empty(), true);
            })
        .put(
            TIMESTAMPADD,
            args -> {
                return TimestampAdd$.MODULE$.apply(args.get(0).toString(), args.get(1), args.get(2), Option.empty());
            })
        .put(
            TIMESTAMPDIFF,
            args -> {
                return TimestampDiff$.MODULE$.apply(args.get(0).toString(), args.get(1), args.get(2), Option.empty());
            })
        .put(
            UTC_TIMESTAMP,
            args -> {
                return ToUTCTimestamp$.MODULE$.apply(CurrentTimestamp$.MODULE$.apply(), CurrentTimeZone$.MODULE$.apply());
            })
        .build();

  /**
   * The mapping between PPL builtin functions to Spark UDF implemented in this project.
   */
  Map<BuiltinFunctionName, Function<List<Expression>, Expression>> PPL_TO_SPARK_UDF_MAPPING
      = ImmutableMap.<BuiltinFunctionName, Function<List<Expression>, Expression>>builder()
      // JSON UDF
      .put(
          JSON_DELETE,
          args -> SerializableUdf.visit(JSON_DELETE, args))
      .put(
          JSON_SET,
          args -> SerializableUdf.visit(JSON_SET, args))
      .put(
          JSON_APPEND,
          args -> SerializableUdf.visit(JSON_APPEND, args))
      .put(
          JSON_EXTEND,
          args -> SerializableUdf.visit(JSON_EXTEND, args))
      // IP Relevance UDF
      .put(
          CIDR,
          args -> SerializableUdf.visit(CIDR, args))
      .put(
          IS_IPV4,
          args -> SerializableUdf.visit(IS_IPV4, args))
      .put(
          IP_TO_INT,
          args -> SerializableUdf.visit(IP_TO_INT, args))
      // Relative Time UDF
      .put(
          RELATIVE_TIMESTAMP,
          args -> buildRelativeTimestamp(args.get(0)))
      .put(
          EARLIEST,
          args -> {
            Expression relativeTimestamp = buildRelativeTimestamp(args.get(0));
            Expression timestamp = args.get(1);
            return LessThanOrEqual$.MODULE$.apply(relativeTimestamp, timestamp);
          })
      .put(
          LATEST,
          args -> {
            Expression relativeTimestamp = buildRelativeTimestamp(args.get(0));
            Expression timestamp = args.get(1);
            return GreaterThanOrEqual$.MODULE$.apply(relativeTimestamp, timestamp);
          })
      .build();

    static Expression builtinFunction(org.opensearch.sql.ast.expression.Function function, List<Expression> args) {
      Optional<BuiltinFunctionName> builtinOpt = BuiltinFunctionName.of(function.getFuncName());
      if (builtinOpt.isEmpty()) {
          throw new UnsupportedOperationException(function.getFuncName() + " is not a builtin function of PPL");
      }
      BuiltinFunctionName builtin = builtinOpt.get();

      // 1. Transform to spark built-int function if there is a direct mapping
      String name = SPARK_BUILTIN_FUNCTION_NAME_MAPPING.get(builtin);
      if (name != null) {
          // there is a Spark builtin function mapping with the PPL builtin function
          return new UnresolvedFunction(seq(name), seq(args), false, empty(),false);
      }

      // 2. Transform to spark built-int function if there is an alternative mapping
      Function<List<Expression>, Expression> alternative = PPL_TO_SPARK_FUNC_MAPPING.get(builtin);
      if (alternative != null) {
          return alternative.apply(args);
      }

      // 3. Transform to spark UDF
      // if we already have a self-defined implementation in this project
      Function<List<Expression>, Expression> udf = PPL_TO_SPARK_UDF_MAPPING.get(builtin);
      if(udf != null) {
          return udf.apply(args);
      }

      // 4. Transform to spark built-int function directly without mapping
      name = builtin.getName().getFunctionName();
      return new UnresolvedFunction(seq(name), seq(args), false, empty(),false);
    }

    static Expression[] createIntervalArgs(IntervalUnit unit, Expression value) {
        Expression[] args = new Expression[7];
        Arrays.fill(args, Literal$.MODULE$.apply(0));
        switch (unit) {
            case YEAR:   args[0] = value; break;
            case MONTH:  args[1] = value; break;
            case WEEK:   args[2] = value; break;
            case DAY:    args[3] = value; break;
            case HOUR:   args[4] = value; break;
            case MINUTE: args[5] = value; break;
            case SECOND: args[6] = value; break;
            default:
                throw new IllegalArgumentException("Unsupported Interval unit: " + unit);
        }
        return args;
    }

    private static Expression buildRelativeTimestamp(Expression relativeStringExpression) {
        return SerializableUdf.visit(
                RELATIVE_TIMESTAMP,
                List.of(relativeStringExpression, CurrentTimestamp$.MODULE$.apply(), CurrentTimeZone$.MODULE$.apply()));
    }
}
