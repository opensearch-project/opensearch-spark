/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

import org.apache.spark.network.util.ByteUnit;
import org.opensearch.flint.core.http.FlintRetryOptions;

/**
 * Flint Options include all the flint related configuration.
 */
public class FlintOptions implements Serializable {

  private final Map<String, String> options;

  /**
   * Flint options related to HTTP retry policy.
   */
  private final FlintRetryOptions retryOptions;

  public static final String HOST = "host";

  public static final String PORT = "port";

  public static final String REGION = "region";

  public static final String DEFAULT_REGION = "us-west-2";

  public static final String SCHEME = "scheme";

  /**
   * Service name used for SigV4 signature.
   * `es`: Amazon OpenSearch Service
   * `aoss`: Amazon OpenSearch Serverless
   */
  public static final String SERVICE_NAME = "auth.servicename";
  public static final String SERVICE_NAME_ES = "es";
  public static final String SERVICE_NAME_AOSS = "aoss";

  public static final String AUTH = "auth";
  public static final String NONE_AUTH = "noauth";
  public static final String SIGV4_AUTH = "sigv4";
  public static final String BASIC_AUTH = "basic";
  public static final String USERNAME = "auth.username";
  public static final String PASSWORD = "auth.password";

  public static final String CUSTOM_AWS_CREDENTIALS_PROVIDER = "customAWSCredentialsProvider";

  public static final String METADATA_ACCESS_AWS_CREDENTIALS_PROVIDER = "spark.metadata.accessAWSCredentialsProvider";

  /**
   * By default, customAWSCredentialsProvider and accessAWSCredentialsProvider are empty. use DefaultAWSCredentialsProviderChain.
   */
  public static final String DEFAULT_AWS_CREDENTIALS_PROVIDER = "";

  public static final String SYSTEM_INDEX_KEY_NAME = "spark.flint.job.requestIndex";

  /**
   * The page size for OpenSearch Rest Request.
   */
  public static final String SCROLL_SIZE = "read.scroll_size";
  public static final int DEFAULT_SCROLL_SIZE = 100;

  public static final String SCROLL_DURATION = "read.scroll_duration";
  /**
   * 5 minutes;
   */
  public static final int DEFAULT_SCROLL_DURATION = 5;

  public static final String REFRESH_POLICY = "write.refresh_policy";
  /**
   * NONE("false")
   *
   * IMMEDIATE("true")
   *
   * WAIT_UNTIL("wait_for")
   */
  public static final String DEFAULT_REFRESH_POLICY = "false";

  public static final String SOCKET_TIMEOUT_MILLIS = "socket_timeout_millis";

  public static final int DEFAULT_SOCKET_TIMEOUT_MILLIS = 60000;

  public static final int DEFAULT_INACTIVITY_LIMIT_MILLIS = 3 * 60 * 1000;

  public static final String REQUEST_COMPLETION_DELAY_MILLIS = "request.completionDelayMillis";
  public static final int DEFAULT_REQUEST_COMPLETION_DELAY_MILLIS = 0;
  public static final int DEFAULT_AOSS_REQUEST_COMPLETION_DELAY_MILLIS = 2000;

  public static final String DATA_SOURCE_NAME = "spark.flint.datasource.name";

  public static final String BATCH_BYTES = "write.batch_bytes";

  public static final String DEFAULT_BATCH_BYTES = "1mb";

  public static final String CUSTOM_FLINT_METADATA_LOG_SERVICE_CLASS = "customFlintMetadataLogServiceClass";

  public static final String CUSTOM_FLINT_INDEX_METADATA_SERVICE_CLASS = "customFlintIndexMetadataServiceClass";

  public static final String CUSTOM_FLINT_SCHEDULER_CLASS = "customFlintSchedulerClass";

  public static final String SUPPORT_SHARD = "read.support_shard";

  public static final String DEFAULT_SUPPORT_SHARD = "true";

  private static final String UNKNOWN = "UNKNOWN";

  public static final String BULK_REQUEST_RATE_LIMIT_PER_NODE_ENABLED = "write.bulk.rate_limit_per_node.enabled";
  public static final String DEFAULT_BULK_REQUEST_RATE_LIMIT_PER_NODE_ENABLED = "false";
  public static final String BULK_REQUEST_MIN_RATE_LIMIT_PER_NODE = "write.bulk.rate_limit_per_node.min";
  public static final String DEFAULT_BULK_REQUEST_MIN_RATE_LIMIT_PER_NODE = "5000";
  public static final String BULK_REQUEST_MAX_RATE_LIMIT_PER_NODE = "write.bulk.rate_limit_per_node.max";
  public static final String DEFAULT_BULK_REQUEST_MAX_RATE_LIMIT_PER_NODE = "50000";
  public static final String BULK_REQUEST_RATE_LIMIT_PER_NODE_INCREASE_STEP = "write.bulk.rate_limit_per_node.increase_step";
  public static final String DEFAULT_BULK_REQUEST_RATE_LIMIT_PER_NODE_INCREASE_STEP = "500";
  public static final String BULK_REQUEST_RATE_LIMIT_PER_NODE_DECREASE_RATIO = "write.bulk.rate_limit_per_node.decrease_ratio";
  public static final String DEFAULT_BULK_REQUEST_RATE_LIMIT_PER_NODE_DECREASE_RATIO = "0.8";

  public static final String DEFAULT_EXTERNAL_SCHEDULER_INTERVAL = "5 minutes";

  public FlintOptions(Map<String, String> options) {
    this.options = options;
    this.retryOptions = new FlintRetryOptions(options);
  }

  public String getHost() {
    return options.getOrDefault(HOST, "localhost");
  }

  public int getPort() {
    return Integer.parseInt(options.getOrDefault(PORT, "9200"));
  }

  public Optional<Integer> getScrollSize() {
    if (options.containsKey(SCROLL_SIZE)) {
      return Optional.of(Integer.parseInt(options.get(SCROLL_SIZE)));
    } else {
      return Optional.empty();
    }
  }

  public int getScrollDuration() {
    return Integer.parseInt(options.getOrDefault(SCROLL_DURATION, String.valueOf(DEFAULT_SCROLL_DURATION)));
  }

  public String getRefreshPolicy() {return options.getOrDefault(REFRESH_POLICY, DEFAULT_REFRESH_POLICY);}

  public FlintRetryOptions getRetryOptions() {
    return retryOptions;
  }

  public String getRegion() {
    return options.getOrDefault(REGION, DEFAULT_REGION);
  }

  public String getScheme() {
    return options.getOrDefault(SCHEME, "http");
  }

  public String getAuth() {
    return options.getOrDefault(AUTH, NONE_AUTH);
  }

  public String getServiceName() {
    return options.getOrDefault(SERVICE_NAME, SERVICE_NAME_ES);
  }

  public String getCustomAwsCredentialsProvider() {
    return options.getOrDefault(CUSTOM_AWS_CREDENTIALS_PROVIDER, DEFAULT_AWS_CREDENTIALS_PROVIDER);
  }

  public String getMetadataAccessAwsCredentialsProvider() {
    return options.getOrDefault(METADATA_ACCESS_AWS_CREDENTIALS_PROVIDER, DEFAULT_AWS_CREDENTIALS_PROVIDER);
  }

  public String getUsername() {
    return options.getOrDefault(USERNAME, "flint");
  }

  public String getPassword() {
    return options.getOrDefault(PASSWORD, "flint");
  }

  public int getSocketTimeoutMillis() {
    return Integer.parseInt(options.getOrDefault(SOCKET_TIMEOUT_MILLIS, String.valueOf(DEFAULT_SOCKET_TIMEOUT_MILLIS)));
  }

  public int getRequestCompletionDelayMillis() {
    int defaultValue = SERVICE_NAME_AOSS.equals(getServiceName())
        ? DEFAULT_AOSS_REQUEST_COMPLETION_DELAY_MILLIS
        : DEFAULT_REQUEST_COMPLETION_DELAY_MILLIS;
    return Integer.parseInt(options.getOrDefault(REQUEST_COMPLETION_DELAY_MILLIS, String.valueOf(defaultValue)));
  }

  public String getDataSourceName() {
    return options.getOrDefault(DATA_SOURCE_NAME, "");
  }
  
  /**
   * Get the AWS accountId from the cluster name.
   * Flint cluster name is in the format of "accountId:clusterName".
   * @return the AWS accountId
   */
  public String getAWSAccountId() {
    String clusterName = System.getenv().getOrDefault("FLINT_CLUSTER_NAME", UNKNOWN + ":" + UNKNOWN);
    String[] parts = clusterName.split(":");
    return parts.length == 2 ? parts[0] : UNKNOWN;
  }

  public String getSystemIndexName() {
    return options.getOrDefault(SYSTEM_INDEX_KEY_NAME, "");
  }

  public int getBatchBytes() {
    // we did not expect this value could be large than 10mb = 10 * 1024 * 1024
    return (int) org.apache.spark.network.util.JavaUtils
        .byteStringAs(options.getOrDefault(BATCH_BYTES, DEFAULT_BATCH_BYTES), ByteUnit.BYTE);
  }

  public String getCustomFlintMetadataLogServiceClass() {
    return options.getOrDefault(CUSTOM_FLINT_METADATA_LOG_SERVICE_CLASS, "");
  }

  public String getCustomFlintIndexMetadataServiceClass() {
    return options.getOrDefault(CUSTOM_FLINT_INDEX_METADATA_SERVICE_CLASS, "");
  }

  /**
   * FIXME, This is workaround for AWS OpenSearch Serverless (AOSS). AOSS does not support shard
   * operation, but shard info is exposed in index settings. Remove this setting when AOSS fix
   * the bug.
   *
   * @return
   */
  public boolean supportShard() {
    return options.getOrDefault(SUPPORT_SHARD, DEFAULT_SUPPORT_SHARD).equalsIgnoreCase(
        DEFAULT_SUPPORT_SHARD);
  }

  public boolean getBulkRequestRateLimitPerNodeEnabled() {
    return Boolean.parseBoolean(options.getOrDefault(BULK_REQUEST_RATE_LIMIT_PER_NODE_ENABLED, DEFAULT_BULK_REQUEST_RATE_LIMIT_PER_NODE_ENABLED));
  }

  public long getBulkRequestMinRateLimitPerNode() {
    return Long.parseLong(options.getOrDefault(BULK_REQUEST_MIN_RATE_LIMIT_PER_NODE, DEFAULT_BULK_REQUEST_MIN_RATE_LIMIT_PER_NODE));
  }

  public long getBulkRequestMaxRateLimitPerNode() {
    return Long.parseLong(options.getOrDefault(BULK_REQUEST_MAX_RATE_LIMIT_PER_NODE, DEFAULT_BULK_REQUEST_MAX_RATE_LIMIT_PER_NODE));
  }

  public long getBulkRequestRateLimitPerNodeIncreaseStep() {
    return Long.parseLong(options.getOrDefault(BULK_REQUEST_RATE_LIMIT_PER_NODE_INCREASE_STEP, DEFAULT_BULK_REQUEST_RATE_LIMIT_PER_NODE_INCREASE_STEP));
  }

  public double getBulkRequestRateLimitPerNodeDecreaseRatio() {
    return Double.parseDouble(options.getOrDefault(BULK_REQUEST_RATE_LIMIT_PER_NODE_DECREASE_RATIO, DEFAULT_BULK_REQUEST_RATE_LIMIT_PER_NODE_DECREASE_RATIO));
  }

  public String getCustomAsyncQuerySchedulerClass() {
    return options.getOrDefault(CUSTOM_FLINT_SCHEDULER_CLASS, "");
  }
}
