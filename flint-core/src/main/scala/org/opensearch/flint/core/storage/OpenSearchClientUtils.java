/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import java.lang.reflect.Constructor;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.Strings;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.flint.core.RestHighLevelClientWrapper;
import org.opensearch.flint.core.auth.ResourceBasedAWSRequestSigningApacheInterceptor;
import org.opensearch.flint.core.http.RetryableHttpAsyncClient;

/**
 * Utility functions to create {@link IRestHighLevelClient}.
 */
public class OpenSearchClientUtils {

  /**
   * Metadata log index name prefix
   */
  public final static String META_LOG_NAME_PREFIX = ".query_execution_request";

  /**
   * Invalid index name characters to percent-encode,
   * excluding '*' because it's reserved for pattern matching.
   */
  private final static Set<Character> INVALID_INDEX_NAME_CHARS =
      Set.of(' ', ',', ':', '"', '+', '/', '\\', '|', '?', '#', '>', '<');

  /**
   * Used in IT.
   */
  public static RestHighLevelClient createRestHighLevelClient(FlintOptions options) {
    RestClientBuilder
        restClientBuilder =
        RestClient.builder(new HttpHost(options.getHost(), options.getPort(), options.getScheme()));

    if (options.getAuth().equals(FlintOptions.SIGV4_AUTH)) {
      restClientBuilder = configureSigV4Auth(restClientBuilder, options);
    } else if (options.getAuth().equals(FlintOptions.BASIC_AUTH)) {
      restClientBuilder = configureBasicAuth(restClientBuilder, options);
    } else {
      restClientBuilder = configureDefaultAuth(restClientBuilder, options);
    }

    final RequestConfigurator callback = new RequestConfigurator(options);
    restClientBuilder.setRequestConfigCallback(callback);

    return new RestHighLevelClient(restClientBuilder);
  }

  public static IRestHighLevelClient createClient(FlintOptions options) {
    return new RestHighLevelClientWrapper(createRestHighLevelClient(options),
        BulkRequestRateLimiterHolder.getBulkRequestRateLimiter(options));
  }

  /**
   * Sanitize index name to comply with OpenSearch index name restrictions.
   */
  public static String sanitizeIndexName(String indexName) {
    Objects.requireNonNull(indexName);

    String encoded = percentEncode(indexName);
    return toLowercase(encoded);
  }

  /**
   * Because OpenSearch requires all lowercase letters in index name, we have to
   * lowercase all letters in the given Flint index name.
   */
  private static String toLowercase(String indexName) {
    Objects.requireNonNull(indexName);

    return indexName.toLowerCase(Locale.ROOT);
  }

  /**
   * Percent-encode invalid OpenSearch index name characters.
   */
  private static String percentEncode(String indexName) {
    Objects.requireNonNull(indexName);

    StringBuilder builder = new StringBuilder(indexName.length());
    for (char ch : indexName.toCharArray()) {
      if (INVALID_INDEX_NAME_CHARS.contains(ch)) {
        builder.append(String.format("%%%02X", (int) ch));
      } else {
        builder.append(ch);
      }
    }
    return builder.toString();
  }

  private static RestClientBuilder configureSigV4Auth(RestClientBuilder restClientBuilder, FlintOptions options) {
    // Use DefaultAWSCredentialsProviderChain by default.
    final AtomicReference<AWSCredentialsProvider> customAWSCredentialsProvider =
        new AtomicReference<>(new DefaultAWSCredentialsProviderChain());
    String customProviderClass = options.getCustomAwsCredentialsProvider();
    if (!Strings.isNullOrEmpty(customProviderClass)) {
      instantiateProvider(customProviderClass, customAWSCredentialsProvider);
    }

    // Set metadataAccessAWSCredentialsProvider to customAWSCredentialsProvider by default for backwards compatibility
    // unless a specific metadata access provider class name is provided
    String metadataAccessProviderClass = options.getMetadataAccessAwsCredentialsProvider();
    final AtomicReference<AWSCredentialsProvider> metadataAccessAWSCredentialsProvider =
        new AtomicReference<>(new DefaultAWSCredentialsProviderChain());

    String metaLogIndexName = constructMetaLogIndexName(options.getDataSourceName());
    String systemIndexName = Strings.isNullOrEmpty(options.getSystemIndexName()) ? metaLogIndexName : options.getSystemIndexName();

    if (Strings.isNullOrEmpty(metadataAccessProviderClass)) {
      metadataAccessAWSCredentialsProvider.set(customAWSCredentialsProvider.get());
    } else {
      instantiateProvider(metadataAccessProviderClass, metadataAccessAWSCredentialsProvider);
    }

    restClientBuilder.setHttpClientConfigCallback(builder -> {
          HttpAsyncClientBuilder delegate = builder.addInterceptorLast(
              new ResourceBasedAWSRequestSigningApacheInterceptor(
                  options.getServiceName(), options.getRegion(), customAWSCredentialsProvider.get(), metadataAccessAWSCredentialsProvider.get(), systemIndexName));
          return RetryableHttpAsyncClient.builder(delegate, options);
        }
    );

    return restClientBuilder;
  }

  private static RestClientBuilder configureBasicAuth(RestClientBuilder restClientBuilder, FlintOptions options) {
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        AuthScope.ANY,
        new UsernamePasswordCredentials(options.getUsername(), options.getPassword()));
    restClientBuilder.setHttpClientConfigCallback(builder -> {
      HttpAsyncClientBuilder delegate = builder.setDefaultCredentialsProvider(credentialsProvider);
      return RetryableHttpAsyncClient.builder(delegate, options);
    });

    return restClientBuilder;
  }

  private static RestClientBuilder configureDefaultAuth(RestClientBuilder restClientBuilder, FlintOptions options) {
    // No auth
    restClientBuilder.setHttpClientConfigCallback(delegate ->
        RetryableHttpAsyncClient.builder(delegate, options));
    return restClientBuilder;
  }

  /**
   * Attempts to instantiate the AWS credential provider using reflection.
   */
  private static void instantiateProvider(String providerClass, AtomicReference<AWSCredentialsProvider> provider) {
    try {
      Class<?> awsCredentialsProviderClass = Class.forName(providerClass);
      Constructor<?> ctor = awsCredentialsProviderClass.getDeclaredConstructor();
      ctor.setAccessible(true);
      provider.set((AWSCredentialsProvider) ctor.newInstance());
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate AWSCredentialsProvider: " + providerClass, e);
    }
  }

  private static String constructMetaLogIndexName(String dataSourceName) {
    return dataSourceName.isEmpty() ? META_LOG_NAME_PREFIX : META_LOG_NAME_PREFIX + "_" + dataSourceName;
  }
}
