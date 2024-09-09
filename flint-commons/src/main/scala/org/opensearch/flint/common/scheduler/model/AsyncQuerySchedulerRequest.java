/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.common.scheduler.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

/** Represents a job request for a scheduled task. */
@Builder
@Data
public class AsyncQuerySchedulerRequest {
    protected String accountId;
    // Scheduler jobid is the opensearch index name until we support multiple jobs per index
    protected String jobId;
    protected String dataSource;
    protected String scheduledQuery;
    protected String queryLang;
    protected Object schedule;
    protected boolean enabled;
    protected Instant lastUpdateTime;
    protected Instant enabledTime;
    protected Long lockDurationSeconds;
    protected Double jitter;
}
