/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry;

import java.util.Map;

/**
 * Utility class for handling Flint metadata log entries in OpenSearch storage.
 */
public class FlintMetadataLogEntryOpenSearchConverter {

  public static final String QUERY_EXECUTION_REQUEST_MAPPING = String.join("\n",
      "{",
      "  \"dynamic\": false,",
      "  \"properties\": {",
      "    \"version\": {",
      "      \"type\": \"keyword\"",
      "    },",
      "    \"type\": {",
      "      \"type\": \"keyword\"",
      "    },",
      "    \"state\": {",
      "      \"type\": \"keyword\"",
      "    },",
      "    \"statementId\": {",
      "      \"type\": \"keyword\"",
      "    },",
      "    \"applicationId\": {",
      "      \"type\": \"keyword\"",
      "    },",
      "    \"sessionId\": {",
      "      \"type\": \"keyword\"",
      "    },",
      "    \"sessionType\": {",
      "      \"type\": \"keyword\"",
      "    },",
      "    \"error\": {",
      "      \"type\": \"text\"",
      "    },",
      "    \"lang\": {",
      "      \"type\": \"keyword\"",
      "    },",
      "    \"query\": {",
      "      \"type\": \"text\"",
      "    },",
      "    \"dataSourceName\": {",
      "      \"type\": \"keyword\"",
      "    },",
      "    \"submitTime\": {",
      "      \"type\": \"date\",",
      "      \"format\": \"strict_date_time||epoch_millis\"",
      "    },",
      "    \"jobId\": {",
      "      \"type\": \"keyword\"",
      "    },",
      "    \"lastUpdateTime\": {",
      "      \"type\": \"date\",",
      "      \"format\": \"strict_date_time||epoch_millis\"",
      "    },",
      "    \"queryId\": {",
      "      \"type\": \"keyword\"",
      "    },",
      "    \"excludeJobIds\": {",
      "      \"type\": \"keyword\"",
      "    }",
      "  }",
      "}");

  public static final String QUERY_EXECUTION_REQUEST_SETTINGS = String.join("\n",
      "{",
      "  \"index\": {",
      "    \"number_of_shards\": \"1\",",
      "    \"auto_expand_replicas\": \"0-2\",",
      "    \"number_of_replicas\": \"0\"",
      "  }",
      "}");

  /**
   * Convert a log entry to json string for persisting to OpenSearch.
   * Expects the following field in properties:
   * - dataSourceName: data source name
   *
   * @param logEntry
   *   log entry to convert
   * @return
   *   json string representation of the log entry
   */
  public static String toJson(FlintMetadataLogEntry logEntry) throws JsonProcessingException {
    String applicationId = System.getenv().getOrDefault("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown");
    String jobId = System.getenv().getOrDefault("SERVERLESS_EMR_JOB_ID", "unknown");
    long lastUpdateTime = System.currentTimeMillis();

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode json = mapper.createObjectNode();

    json.put("version", "1.1");
    json.put("latestId", logEntry.id());
    json.put("type", "flintindexstate");
    json.put("state", logEntry.state().toString());
    json.put("applicationId", applicationId);
    json.put("jobId", jobId);
    json.put("dataSourceName", logEntry.properties().get("dataSourceName").get().toString());
    json.put("jobStartTime", logEntry.createTime());
    json.put("lastRefreshStartTime", logEntry.lastRefreshStartTime());
    json.put("lastRefreshCompleteTime", logEntry.lastRefreshCompleteTime());
    json.put("lastUpdateTime", lastUpdateTime);
    json.put("error", logEntry.error());

    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
  }

  /**
   * Construct a log entry from OpenSearch document fields.
   *
   * @param id
   *   OpenSearch document id
   * @param seqNo
   *   OpenSearch document sequence number
   * @param primaryTerm
   *   OpenSearch document primary term
   * @param sourceMap
   *   OpenSearch document source as a map
   * @return
   *   log entry
   */
  public static FlintMetadataLogEntry constructLogEntry(
      String id,
      Long seqNo,
      Long primaryTerm,
      Map<String, Object> sourceMap) {
    return new FlintMetadataLogEntry(
        id,
        /* sourceMap may use Integer or Long even though it's always long in index mapping */
        ((Number) sourceMap.get("jobStartTime")).longValue(),
        ((Number) sourceMap.get("lastRefreshStartTime")).longValue(),
        ((Number) sourceMap.get("lastRefreshCompleteTime")).longValue(),
        FlintMetadataLogEntry.IndexState$.MODULE$.from((String) sourceMap.get("state")),
        Map.of("seqNo", seqNo, "primaryTerm", primaryTerm),
        (String) sourceMap.get("error"),
        Map.of("dataSourceName", (String) sourceMap.get("dataSourceName")));
  }
}
