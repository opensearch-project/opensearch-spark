/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.common.scheduler.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

/**
 * Represents a job request for a scheduled task.
 */
@Builder
@Data
public class AsyncQuerySchedulerRequest {

    /**
     * The AWS accountId used to identify the resource.
     */
    String accountId;

    /**
     * The unique identifier for the scheduler job.
     */
    String jobId;

    /**
     * The name of the data source on which the scheduled query will be executed.
     */
    String dataSource;

    /**
     * The scheduled query to be executed.
     */
    String scheduledQuery;

    /**
     * The language in which the query is written, such as SQL, PPL (Piped Processing Language), etc.
     */
    String queryLang;

    /**
     * The interval expression defining the frequency of the job execution.
     * Typically expressed as a time-based pattern (e.g. 5 minutes).
     */
    String interval;

    /**
     * Indicates whether the scheduled job is currently enabled or not.
     */
    boolean enabled;

    /**
     * The timestamp of the last update made to this job.
     */
    Instant lastUpdateTime;

    /**
     * The timestamp when this job was enabled.
     */
    Instant enabledTime;

    /**
     * The duration, in seconds, for which the job remains locked.
     * This lock is used to prevent concurrent executions of the same job, ensuring that only one instance of the job runs at a time.
     */
    Long lockDurationSeconds;

    /**
     * The jitter value to add randomness to the execution schedule, helping to avoid the thundering herd problem.
     */
    Double jitter;
}