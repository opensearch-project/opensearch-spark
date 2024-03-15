/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import dev.failsafe.RetryPolicy;
import java.io.Serializable;
import java.util.Map;
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

  public static final String AUTH = "auth";

  public static final String NONE_AUTH = "noauth";

  public static final String SIGV4_AUTH = "sigv4";

  public static final String BASIC_AUTH = "basic";

  public static final String USERNAME = "auth.username";

  public static final String PASSWORD = "auth.password";

  public static final String CUSTOM_AWS_CREDENTIALS_PROVIDER = "customAWSCredentialsProvider";

  /**
   * By default, customAWSCredentialsProvider is empty. use DefaultAWSCredentialsProviderChain.
   */
  public static final String DEFAULT_CUSTOM_AWS_CREDENTIALS_PROVIDER = "";

  /**
   * Used by {@link org.opensearch.flint.core.storage.OpenSearchScrollReader}
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
  public static final String DEFAULT_REFRESH_POLICY = "wait_for";

  public static final String SOCKET_TIMEOUT_MILLIS = "socket_timeout_millis";

  public static final int DEFAULT_SOCKET_TIMEOUT_MILLIS = 60000;

  public static final int DEFAULT_INACTIVITY_LIMIT_MILLIS = 10 * 60 * 1000;
  
  public static final String DATA_SOURCE_NAME = "spark.flint.datasource.name";

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

  public int getScrollSize() {
    return Integer.parseInt(options.getOrDefault(SCROLL_SIZE, String.valueOf(DEFAULT_SCROLL_SIZE)));
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

  public String getCustomAwsCredentialsProvider() {
    return options.getOrDefault(CUSTOM_AWS_CREDENTIALS_PROVIDER, "");
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

  public String getDataSourceName() {
    return options.getOrDefault(DATA_SOURCE_NAME, "");
  }
}
