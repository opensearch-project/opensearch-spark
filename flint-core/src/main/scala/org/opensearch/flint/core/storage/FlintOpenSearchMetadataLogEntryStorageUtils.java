/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry;

import java.util.Map;

public class FlintOpenSearchMetadataLogEntryStorageUtils {

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

  // TODO: move dataSourceName to entry
  public static String toJson(FlintMetadataLogEntry logEntry, String dataSourceName) {
    String applicationId = System.getenv().getOrDefault("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown");
    String jobId = System.getenv().getOrDefault("SERVERLESS_EMR_JOB_ID", "unknown");
    long lastUpdateTime = System.currentTimeMillis();

    return String.format(
        "{\n" +
            "  \"version\": \"1.0\",\n" +
            "  \"latestId\": \"%s\",\n" +
            "  \"type\": \"flintindexstate\",\n" +
            "  \"state\": \"%s\",\n" +
            "  \"applicationId\": \"%s\",\n" +
            "  \"jobId\": \"%s\",\n" +
            "  \"dataSourceName\": \"%s\",\n" +
            "  \"jobStartTime\": %d,\n" +
            "  \"lastUpdateTime\": %d,\n" +
            "  \"error\": \"%s\"\n" +
            "}",
        logEntry.id(), logEntry.state(), applicationId, jobId, dataSourceName, logEntry.createTime(), lastUpdateTime, logEntry.error());
  }

  public static FlintMetadataLogEntry constructLogEntry(
      String id,
      Long seqNo,
      Long primaryTerm,
      Map<String, Object> sourceMap) {
    return new FlintMetadataLogEntry(
        id,
        /* getSourceAsMap() may use Integer or Long even though it's always long in index mapping */
        (Long) sourceMap.get("jobStartTime"),
        FlintMetadataLogEntry.IndexState$.MODULE$.from((String) sourceMap.get("state")),
        Map.of("seqNo", seqNo, "primaryTerm", primaryTerm),
        (String) sourceMap.get("error"));
  }
}
