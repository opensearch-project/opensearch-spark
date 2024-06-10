/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import java.lang.reflect.Constructor;
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
 * {@link IRestHighLevelClient} factory.
 */
public class OpenSearchClientFactory {

  private static final String SERVICE_NAME = "es";

  /**
   * Metadata log index name prefix
   */
  public final static String META_LOG_NAME_PREFIX = ".query_execution_request";

  public static IRestHighLevelClient createClient(FlintOptions options) {
    RestClientBuilder
        restClientBuilder =
        RestClient.builder(new HttpHost(options.getHost(), options.getPort(), options.getScheme()));

    // SigV4 support
    if (options.getAuth().equals(FlintOptions.SIGV4_AUTH)) {
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
                    SERVICE_NAME, options.getRegion(), customAWSCredentialsProvider.get(), metadataAccessAWSCredentialsProvider.get(), systemIndexName));
            return RetryableHttpAsyncClient.builder(delegate, options);
          }
      );
    } else if (options.getAuth().equals(FlintOptions.BASIC_AUTH)) {
      CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(
          AuthScope.ANY,
          new UsernamePasswordCredentials(options.getUsername(), options.getPassword()));
      restClientBuilder.setHttpClientConfigCallback(builder -> {
        HttpAsyncClientBuilder delegate = builder.setDefaultCredentialsProvider(credentialsProvider);
        return RetryableHttpAsyncClient.builder(delegate, options);
      });
    } else {
      restClientBuilder.setHttpClientConfigCallback(delegate ->
          RetryableHttpAsyncClient.builder(delegate, options));
    }

    final RequestConfigurator callback = new RequestConfigurator(options);
    restClientBuilder.setRequestConfigCallback(callback);

    return new RestHighLevelClientWrapper(new RestHighLevelClient(restClientBuilder));
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
