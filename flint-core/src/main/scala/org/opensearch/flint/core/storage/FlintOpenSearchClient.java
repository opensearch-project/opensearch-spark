/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import static org.opensearch.common.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.core.FlintClient;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.auth.AWSRequestSigningApacheInterceptor;
import org.opensearch.flint.core.metadata.FlintMetadata;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Flint client implementation for OpenSearch storage.
 */
public class FlintOpenSearchClient implements FlintClient {

  /**
   * {@link NamedXContentRegistry} from {@link SearchModule} used for construct {@link QueryBuilder} from DSL query string.
   */
  private final static NamedXContentRegistry
      xContentRegistry =
      new NamedXContentRegistry(new SearchModule(Settings.builder().build(),
          new ArrayList<>()).getNamedXContents());

  private final FlintOptions options;

  public FlintOpenSearchClient(FlintOptions options) {
    this.options = options;
  }

  @Override public void createIndex(String indexName, FlintMetadata metadata) {
    try (RestHighLevelClient client = createClient()) {
      CreateIndexRequest request = new CreateIndexRequest(indexName);
      request.mapping(metadata.getContent(), XContentType.JSON);

      client.indices().create(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create Flint index " + indexName, e);
    }
  }

  @Override public boolean exists(String indexName) {
    try (RestHighLevelClient client = createClient()) {
      return client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to check if Flint index exists " + indexName, e);
    }
  }

  @Override public List<FlintMetadata> getAllIndexMetadata(String indexNamePattern) {
    try (RestHighLevelClient client = createClient()) {
      GetMappingsRequest request = new GetMappingsRequest().indices(indexNamePattern);
      GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);

      return response.mappings().values().stream()
          .map(mapping -> new FlintMetadata(mapping.source().string()))
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get Flint index metadata for " + indexNamePattern, e);
    }
  }

  @Override public FlintMetadata getIndexMetadata(String indexName) {
    try (RestHighLevelClient client = createClient()) {
      GetMappingsRequest request = new GetMappingsRequest().indices(indexName);
      GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);

      MappingMetadata mapping = response.mappings().get(indexName);
      return new FlintMetadata(mapping.source().string());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get Flint index metadata for " + indexName, e);
    }
  }

  @Override public void deleteIndex(String indexName) {
    try (RestHighLevelClient client = createClient()) {
      DeleteIndexRequest request = new DeleteIndexRequest(indexName);

      client.indices().delete(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to delete Flint index " + indexName, e);
    }
  }

  /**
   * Create {@link FlintReader}.
   *
   * @param indexName index name.
   * @param query DSL query. DSL query is null means match_all.
   * @return {@link FlintReader}.
   */
  @Override public FlintReader createReader(String indexName, String query) {
    try {
      QueryBuilder queryBuilder = new MatchAllQueryBuilder();
      if (!Strings.isNullOrEmpty(query)) {
        XContentParser
            parser =
            XContentType.JSON.xContent().createParser(xContentRegistry, IGNORE_DEPRECATIONS, query);
        queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
      }
      return new OpenSearchScrollReader(createClient(),
          indexName,
          new SearchSourceBuilder().query(queryBuilder),
          options);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public FlintWriter createWriter(String indexName) {
    return new OpenSearchWriter(createClient(), indexName, options.getRefreshPolicy());
  }

  private RestHighLevelClient createClient() {
    RestClientBuilder
        restClientBuilder =
        RestClient.builder(new HttpHost(options.getHost(), options.getPort(), options.getScheme()));

    // SigV4 support
    if (options.getAuth().equals(FlintOptions.SIGV4_AUTH)) {
      AWS4Signer signer = new AWS4Signer();
      signer.setServiceName("es");
      signer.setRegionName(options.getRegion());

      // Use DefaultAWSCredentialsProviderChain by default.
      final AtomicReference<AWSCredentialsProvider> awsCredentialsProvider =
          new AtomicReference<>(new DefaultAWSCredentialsProviderChain());
      String providerClass = options.getCustomAwsCredentialsProvider();
      if (!Strings.isNullOrEmpty(providerClass)) {
        try {
          Class<?> awsCredentialsProviderClass = Class.forName(providerClass);
          Constructor<?> ctor = awsCredentialsProviderClass.getDeclaredConstructor();
          ctor.setAccessible(true);
          awsCredentialsProvider.set((AWSCredentialsProvider) ctor.newInstance());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      restClientBuilder.setHttpClientConfigCallback(cb ->
          cb.addInterceptorLast(new AWSRequestSigningApacheInterceptor(signer.getServiceName(),
              signer, awsCredentialsProvider.get())));
    } else if (options.getAuth().equals(FlintOptions.BASIC_AUTH)) {
      CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(
          AuthScope.ANY,
          new UsernamePasswordCredentials(options.getUsername(), options.getPassword()));
      restClientBuilder.setHttpClientConfigCallback(
          httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
    }
    return new RestHighLevelClient(restClientBuilder);
  }
}
