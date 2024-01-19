package org.opensearch.flint.core.metrics.aop;

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

import java.util.logging.Logger;


@Aspect
public class RestClientMetricsAspect {

    private static final Logger LOG = Logger.getLogger(RestClientMetricsAspect.class.getName());

    @Pointcut("execution(* org.opensearch.client.RestHighLevelClient.*(..))")
    public void onRestHighLevelClientMethod() {}

    @Around("onRestHighLevelClientMethod()")
    public Object aroundRestHighLevelClientMethod(ProceedingJoinPoint pjp) throws Throwable {
        long startTime = System.currentTimeMillis();

        try {
            Object response = pjp.proceed();
            long duration = System.currentTimeMillis() - startTime;
            logMetric("success", duration);
            return response;
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            logMetric("failure", duration);
            throw e;
        }
    }

    private void logMetric(String status, long duration) {
        LOG.info("Request " + status + ": " + duration + "ms");
    }
}
