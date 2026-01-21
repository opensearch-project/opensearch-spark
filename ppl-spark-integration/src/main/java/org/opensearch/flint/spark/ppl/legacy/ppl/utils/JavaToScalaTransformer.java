/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.ppl.utils;


import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

public interface JavaToScalaTransformer {
    static <T> PartialFunction<T, T> toPartialFunction(
        java.util.function.Predicate<T> isDefinedAt,
        java.util.function.Function<T, T> apply) {
        return new AbstractPartialFunction<T, T>() {
            @Override
            public boolean isDefinedAt(T t) {
                return isDefinedAt.test(t);
            }

            @Override
            public T apply(T t) {
                if (isDefinedAt.test(t)) return apply.apply(t);
                else return t;
            }
        };
    }
}
