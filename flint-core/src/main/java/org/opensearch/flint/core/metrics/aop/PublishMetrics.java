package org.opensearch.flint.core.metrics.aop;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * This annotation is used to mark methods or types that publish metrics.
 * It is retained at runtime, allowing the application to utilize reflection to
 * discover and handle these metrics accordingly. The annotation can be applied
 * to both methods and types (classes or interfaces).
 *
 * @Retention(RetentionPolicy.RUNTIME) - Indicates that the annotation is available at runtime for reflection.
 * @Target({ElementType.METHOD, ElementType.TYPE}) - Specifies that this annotation can be applied to methods or types.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface PublishMetrics {

    /**
     * Defines the prefix of the metric name.
     * This prefix is used to categorize the metrics and typically represents a specific aspect or feature of Flint.
     *
     * @return the metric name prefix.
     */
    String metricNamePrefix();
}
