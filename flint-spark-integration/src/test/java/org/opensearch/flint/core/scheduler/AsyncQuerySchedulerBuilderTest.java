/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.scheduler;

import org.junit.Test;
import org.opensearch.flint.common.scheduler.AsyncQueryScheduler;
import org.opensearch.flint.common.scheduler.model.AsyncQuerySchedulerRequest;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.spark.scheduler.AsyncQuerySchedulerBuilder;
import org.opensearch.flint.spark.scheduler.OpenSearchAsyncQueryScheduler;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AsyncQuerySchedulerBuilderTest {

    @Test
    public void testBuildWithEmptyClassName() {
        FlintOptions options = mock(FlintOptions.class);
        when(options.getCustomAsyncQuerySchedulerClass()).thenReturn("");

        AsyncQueryScheduler scheduler = AsyncQuerySchedulerBuilder.build(options);
        assertTrue(scheduler instanceof OpenSearchAsyncQueryScheduler);
    }

    @Test
    public void testBuildWithCustomClassName() {
        FlintOptions options = mock(FlintOptions.class);
        when(options.getCustomAsyncQuerySchedulerClass()).thenReturn("org.opensearch.flint.core.scheduler.AsyncQuerySchedulerBuilderTest$AsyncQuerySchedulerForLocalTest");

        AsyncQueryScheduler scheduler = AsyncQuerySchedulerBuilder.build(options);
        assertTrue(scheduler instanceof AsyncQuerySchedulerForLocalTest);
    }

    @Test(expected = RuntimeException.class)
    public void testBuildWithInvalidClassName() {
        FlintOptions options = mock(FlintOptions.class);
        when(options.getCustomAsyncQuerySchedulerClass()).thenReturn("invalid.ClassName");

        AsyncQuerySchedulerBuilder.build(options);
    }

    public static class AsyncQuerySchedulerForLocalTest implements AsyncQueryScheduler {
        @Override
        public void scheduleJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest) {
            // Custom implementation
        }

        @Override
        public void updateJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest) {
            // Custom implementation
        }

        @Override
        public void unscheduleJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest) {
            // Custom implementation
        }

        @Override
        public void removeJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest) {
            // Custom implementation
        }
    }
}