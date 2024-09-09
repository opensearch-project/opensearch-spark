/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.common.scheduler.AsyncQueryScheduler;
import org.opensearch.flint.common.scheduler.model.AsyncQuerySchedulerRequest;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.flint.core.scheduler.util.IntervalSchedulerParser;
import org.opensearch.flint.core.storage.OpenSearchClientUtils;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.rest.RestStatus;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class OpenSearchAsyncQueryScheduler implements AsyncQueryScheduler {
    public static final String SCHEDULER_INDEX_NAME = ".async-query-scheduler";
    private static final String SCHEDULER_INDEX_MAPPING_FILE_NAME = "async-query-scheduler-index-mapping.yml";
    private static final String SCHEDULER_INDEX_SETTINGS_FILE_NAME = "async-query-scheduler-index-settings.yml";
    private static final Logger LOG = LogManager.getLogger(OpenSearchAsyncQueryScheduler.class);

    private static final ObjectMapper mapper = new ObjectMapper();
    private final FlintOptions flintOptions;

    public OpenSearchAsyncQueryScheduler(FlintOptions options) {
        this.flintOptions = options;
    }

    @Override
    public void scheduleJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest) {
        try {
            IRestHighLevelClient client = createClient();
            ensureIndexExists(client);
            indexJob(asyncQuerySchedulerRequest, client);
        } catch (Exception e) {
            handleException("Failed to schedule job", asyncQuerySchedulerRequest.getJobId(), e);
        }
    }

    @Override
    public void unscheduleJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest) {
        try {
            IRestHighLevelClient client = createClient();
            ensureIndexExists(client);
            disableJob(asyncQuerySchedulerRequest, client);
        } catch (Exception e) {
            handleException("Failed to unschedule job", asyncQuerySchedulerRequest.getJobId(), e);
        }
    }

    @Override
    public void updateJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest) {
        try {
            IRestHighLevelClient client = createClient();
            ensureIndexExists(client);
            upsertJob(asyncQuerySchedulerRequest, client);
        } catch (Exception e) {
            handleException("Failed to update job", asyncQuerySchedulerRequest.getJobId(), e);
        }
    }

    @Override
    public void removeJob(AsyncQuerySchedulerRequest request) {
        try {
            validateJobId(request.getJobId());
            IRestHighLevelClient client = createClient();
            ensureIndexExists(client);
            deleteJob(request.getJobId(), client);
        } catch (Exception e) {
            handleException("Failed to remove job", request.getJobId(), e);
        }
    }

    @VisibleForTesting
    void createAsyncQuerySchedulerIndex(IRestHighLevelClient client) {
        try (InputStream mappingFileStream = getResourceAsStream(SCHEDULER_INDEX_MAPPING_FILE_NAME);
             InputStream settingsFileStream = getResourceAsStream(SCHEDULER_INDEX_SETTINGS_FILE_NAME)) {

            CreateIndexRequest createIndexRequest = new CreateIndexRequest(SCHEDULER_INDEX_NAME)
                    .mapping(IOUtils.toString(mappingFileStream, StandardCharsets.UTF_8), XContentType.YAML)
                    .settings(IOUtils.toString(settingsFileStream, StandardCharsets.UTF_8), XContentType.YAML);

            CreateIndexResponse createIndexResponse = client.createIndex(createIndexRequest, RequestOptions.DEFAULT);
            if (!createIndexResponse.isAcknowledged()) {
                throw new RuntimeException("Index creation is not acknowledged.");
            }

            LOG.debug("Index: {} creation acknowledged", SCHEDULER_INDEX_NAME);
        } catch (Throwable e) {
            handleException("Error creating index", SCHEDULER_INDEX_NAME, e);
        }
    }

    private void ensureIndexExists(IRestHighLevelClient client) {
        try {
            if (!client.doesIndexExist(new GetIndexRequest(SCHEDULER_INDEX_NAME), RequestOptions.DEFAULT)) {
                createAsyncQuerySchedulerIndex(client);
            }
        } catch (Throwable e) {
            handleException("Failed to check/create index", SCHEDULER_INDEX_NAME, e);
        }
    }

    private void indexJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest, IRestHighLevelClient client) throws Exception {
        IndexRequest indexRequest = buildIndexRequest(asyncQuerySchedulerRequest);
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        handleIndexResponse(indexResponse, asyncQuerySchedulerRequest.getJobId());
    }

    private void disableJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest, IRestHighLevelClient client) throws Exception {
        asyncQuerySchedulerRequest.setEnabled(false);
        asyncQuerySchedulerRequest.setLastUpdateTime(Instant.now());
        UpdateRequest updateRequest = buildUpdateRequest(asyncQuerySchedulerRequest);
        DocWriteResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        handleUpdateResponse(updateResponse, asyncQuerySchedulerRequest.getJobId(), "unscheduled");
    }

    private void upsertJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest, IRestHighLevelClient client) throws Exception {
        asyncQuerySchedulerRequest.setLastUpdateTime(Instant.now());
        UpdateRequest updateRequest = buildUpdateRequest(asyncQuerySchedulerRequest);
        updateRequest.docAsUpsert(true);
        DocWriteResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        handleUpdateResponse(updateResponse, asyncQuerySchedulerRequest.getJobId(), "updated or created");
    }

    private void deleteJob(String jobId, IRestHighLevelClient client) throws Exception {
        DeleteRequest deleteRequest = new DeleteRequest(SCHEDULER_INDEX_NAME, jobId);
        deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        handleDeleteResponse(deleteResponse, jobId);
    }

    private IndexRequest buildIndexRequest(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest) {
        return new IndexRequest(SCHEDULER_INDEX_NAME)
                .id(asyncQuerySchedulerRequest.getJobId())
                .opType(DocWriteRequest.OpType.CREATE)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(serializeRequest(asyncQuerySchedulerRequest), XContentType.JSON);
    }

    private UpdateRequest buildUpdateRequest(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest) {
        return new UpdateRequest(SCHEDULER_INDEX_NAME, asyncQuerySchedulerRequest.getJobId())
                .doc(serializeRequest(asyncQuerySchedulerRequest), XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
    }

    private void handleIndexResponse(IndexResponse indexResponse, String jobId) {
        if (!indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
            throw new RuntimeException("Schedule job failed with result: " + indexResponse.getResult().getLowercase());
        }
        LOG.debug("Job: {} successfully created", jobId);
    }

    private void handleUpdateResponse(DocWriteResponse updateResponse, String jobId, String action) {
        if (!(updateResponse.getResult().equals(DocWriteResponse.Result.UPDATED) ||
                updateResponse.getResult().equals(DocWriteResponse.Result.CREATED) ||
                updateResponse.getResult().equals(DocWriteResponse.Result.NOOP))) {
            throw new RuntimeException("Update job failed with result: " + updateResponse.getResult().getLowercase());
        }
        LOG.info("Job: {} successfully {}", jobId, action);
    }

    private void handleDeleteResponse(DeleteResponse deleteResponse, String jobId) {
        if (deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED)) {
            LOG.debug("Job: {} successfully deleted", jobId);
        } else if (deleteResponse.getResult().equals(DocWriteResponse.Result.NOT_FOUND)) {
            throw new IllegalArgumentException("Job: " + jobId + " doesn't exist");
        } else {
            throw new RuntimeException("Remove job failed with result: " + deleteResponse.getResult().getLowercase());
        }
    }

    private void handleOpenSearchStatusException(OpenSearchStatusException e, String jobId) {
        if (e.status() == RestStatus.CONFLICT) {
            throw new IllegalArgumentException("A job already exists with name: " + jobId, e);
        } else if (e.status() == RestStatus.NOT_FOUND) {
            throw new IllegalArgumentException("Job: " + jobId + " doesn't exist", e);
        } else {
            throw new RuntimeException("Unexpected OpenSearch exception while handling job: " + jobId, e);
        }
    }

    private void handleException(String message, String identifier, Throwable e) {
        if (e instanceof IllegalArgumentException) {
            throw (IllegalArgumentException) e;
        } else if (e instanceof OpenSearchStatusException) {
            handleOpenSearchStatusException((OpenSearchStatusException) e, identifier);
        } else {
            LOG.error("{} : {}", message, identifier, e);
            throw new RuntimeException(message + " : " + identifier, e);
        }
    }

    private IRestHighLevelClient createClient() {
        return OpenSearchClientUtils.createClient(flintOptions);
    }

    private static String serializeRequest(AsyncQuerySchedulerRequest request) {
        try {
            ObjectNode json = mapper.createObjectNode();
            json.put("accountId", request.getAccountId());
            json.put("jobId", request.getJobId());
            json.put("dataSource", request.getDataSource());
            json.put("scheduledQuery", request.getScheduledQuery());
            json.put("queryLang", request.getQueryLang());
            json.put("enabled", request.isEnabled());
            json.put("lastUpdateTime", request.getLastUpdateTime().toEpochMilli());

            if (request.getEnabledTime() != null) {
                json.put("enabledTime", request.getEnabledTime().toEpochMilli());
            }

            if (request.getLockDurationSeconds() != null) {
                json.put("lockDurationSeconds", request.getLockDurationSeconds());
            }

            if (request.getJitter() != null) {
                json.put("jitter", request.getJitter());
            }

            if (request.getSchedule() != null) {
                Schedule parsedSchedule = IntervalSchedulerParser.parse(request.getSchedule());
                json.set("schedule", serializeSchedule(parsedSchedule));
            }

            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize AsyncQuerySchedulerRequest", e);
        }
    }

    private static ObjectNode serializeSchedule(Schedule schedule) {
        ObjectNode scheduleNode = mapper.createObjectNode();

        if (schedule instanceof IntervalSchedule) {
            IntervalSchedule intervalSchedule = (IntervalSchedule) schedule;

            ObjectNode intervalNode = mapper.createObjectNode();
            intervalNode.put("period", intervalSchedule.getInterval());
            intervalNode.put("unit", intervalSchedule.getUnit().name());
            intervalNode.put("start_time", intervalSchedule.getStartTime().toEpochMilli());

            scheduleNode.set("interval", intervalNode);
        }

        return scheduleNode;
    }

    private InputStream getResourceAsStream(String fileName) {
        return OpenSearchAsyncQueryScheduler.class.getClassLoader().getResourceAsStream(fileName);
    }

    private void validateJobId(String jobId) {
        if (Strings.isNullOrEmpty(jobId)) {
            throw new IllegalArgumentException("JobId cannot be null or empty");
        }
    }
}