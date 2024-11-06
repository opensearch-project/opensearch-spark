/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.scheduler;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SQLContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.flint.common.scheduler.AsyncQueryScheduler;
import org.opensearch.flint.common.scheduler.model.AsyncQuerySchedulerRequest;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.spark.scheduler.AsyncQuerySchedulerBuilder;
import org.opensearch.flint.spark.scheduler.OpenSearchAsyncQueryScheduler;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AsyncQuerySchedulerBuilderTest {
    @Mock
    private SparkSession sparkSession;

    @Mock
    private SQLContext sqlContext;

    private AsyncQuerySchedulerBuilderForLocalTest testBuilder;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(sparkSession.sqlContext()).thenReturn(sqlContext);
    }

    @Test
    public void testBuildWithEmptyClassNameAndAccessibleIndex() throws IOException {
        FlintOptions options = mock(FlintOptions.class);
        when(options.getCustomAsyncQuerySchedulerClass()).thenReturn("");
        OpenSearchAsyncQueryScheduler mockScheduler = mock(OpenSearchAsyncQueryScheduler.class);

        AsyncQueryScheduler scheduler = testBuilder.build(mockScheduler, true, sparkSession, options);
        assertTrue(scheduler instanceof OpenSearchAsyncQueryScheduler);
        verify(sqlContext, never()).setConf(anyString(), anyString());
    }

    @Test
    public void testBuildWithEmptyClassNameAndInaccessibleIndex() throws IOException {
        FlintOptions options = mock(FlintOptions.class);
        when(options.getCustomAsyncQuerySchedulerClass()).thenReturn("");
        OpenSearchAsyncQueryScheduler mockScheduler = mock(OpenSearchAsyncQueryScheduler.class);

        AsyncQueryScheduler scheduler = testBuilder.build(mockScheduler, false, sparkSession, options);
        assertTrue(scheduler instanceof OpenSearchAsyncQueryScheduler);
        verify(sqlContext).setConf("spark.flint.job.externalScheduler.enabled", "false");
    }

    @Test
    public void testBuildWithCustomClassName() throws IOException {
        FlintOptions options = mock(FlintOptions.class);
        when(options.getCustomAsyncQuerySchedulerClass())
                .thenReturn("org.opensearch.flint.core.scheduler.AsyncQuerySchedulerBuilderTest$AsyncQuerySchedulerForLocalTest");

        AsyncQueryScheduler scheduler = AsyncQuerySchedulerBuilder.build(sparkSession, options);
        assertTrue(scheduler instanceof AsyncQuerySchedulerForLocalTest);
    }

    @Test(expected = RuntimeException.class)
    public void testBuildWithInvalidClassName() throws IOException {
        FlintOptions options = mock(FlintOptions.class);
        when(options.getCustomAsyncQuerySchedulerClass()).thenReturn("invalid.ClassName");

        AsyncQuerySchedulerBuilder.build(sparkSession, options);
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

    public static class OpenSearchAsyncQuerySchedulerForLocalTest extends OpenSearchAsyncQueryScheduler {
        @Override
        public boolean hasAccessToSchedulerIndex() {
            return true;
        }
    }

    public static class AsyncQuerySchedulerBuilderForLocalTest extends AsyncQuerySchedulerBuilder {
        private OpenSearchAsyncQueryScheduler mockScheduler;
        private Boolean mockHasAccess;

        public AsyncQuerySchedulerBuilderForLocalTest(OpenSearchAsyncQueryScheduler mockScheduler, Boolean mockHasAccess) {
            this.mockScheduler = mockScheduler;
            this.mockHasAccess = mockHasAccess;
        }

        @Override
        protected OpenSearchAsyncQueryScheduler createOpenSearchAsyncQueryScheduler(FlintOptions options) {
            return mockScheduler != null ? mockScheduler : super.createOpenSearchAsyncQueryScheduler(options);
        }

        @Override
        protected boolean hasAccessToSchedulerIndex(OpenSearchAsyncQueryScheduler scheduler) throws IOException {
            return mockHasAccess != null ? mockHasAccess : super.hasAccessToSchedulerIndex(scheduler);
        }

        public static AsyncQueryScheduler build(OpenSearchAsyncQueryScheduler asyncQueryScheduler, Boolean hasAccess, SparkSession sparkSession, FlintOptions options) throws IOException {
            return new AsyncQuerySchedulerBuilderForLocalTest(asyncQueryScheduler, hasAccess).doBuild(sparkSession, options);
        }
    }
}