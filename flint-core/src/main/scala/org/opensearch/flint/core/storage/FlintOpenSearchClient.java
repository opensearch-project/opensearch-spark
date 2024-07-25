/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import com.google.common.base.Strings;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.core.FlintClient;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.flint.core.metadata.FlintMetadata;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchModule;
import scala.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.opensearch.common.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS;

/**
 * Flint client implementation for OpenSearch storage.
 */
public class FlintOpenSearchClient implements FlintClient {

  private static final Logger LOG = Logger.getLogger(FlintOpenSearchClient.class.getName());


  /**
   * {@link NamedXContentRegistry} from {@link SearchModule} used for construct {@link QueryBuilder} from DSL query string.
   */
  public final static NamedXContentRegistry
      xContentRegistry =
      new NamedXContentRegistry(new SearchModule(Settings.builder().build(),
          new ArrayList<>()).getNamedXContents());

  /**
   * Invalid index name characters to percent-encode,
   * excluding '*' because it's reserved for pattern matching.
   */
  private final static Set<Character> INVALID_INDEX_NAME_CHARS =
      Set.of(' ', ',', ':', '"', '+', '/', '\\', '|', '?', '#', '>', '<');


  private final FlintOptions options;

  public FlintOpenSearchClient(FlintOptions options) {
    this.options = options;
  }

  public static QueryBuilder queryBuilder(String query) throws IOException {
    QueryBuilder queryBuilder = new MatchAllQueryBuilder();
    if (!Strings.isNullOrEmpty(query)) {
      XContentParser
          parser =
          XContentType.JSON.xContent().createParser(xContentRegistry, IGNORE_DEPRECATIONS, query);
      queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
    }
    return queryBuilder;
  }

  @Override
  public void createIndex(String indexName, FlintMetadata metadata) {
    LOG.info("Creating Flint index " + indexName + " with metadata " + metadata);
    createIndex(indexName, metadata.getContent(), metadata.indexSettings());
  }

  protected void createIndex(String indexName, String mapping, Option<String> settings) {
    LOG.info("Creating Flint index " + indexName);
    String osIndexName = sanitizeIndexName(indexName);
    try (IRestHighLevelClient client = createClient()) {
      CreateIndexRequest request = new CreateIndexRequest(osIndexName);
      request.mapping(mapping, XContentType.JSON);
      if (settings.isDefined()) {
        request.settings(settings.get(), XContentType.JSON);
      }
      client.createIndex(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create Flint index " + osIndexName, e);
    }
  }

  @Override
  public boolean exists(String indexName) {
    LOG.info("Checking if Flint index exists " + indexName);
    String osIndexName = sanitizeIndexName(indexName);
    try (IRestHighLevelClient client = createClient()) {
      return client.doesIndexExist(new GetIndexRequest(osIndexName), RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to check if Flint index exists " + osIndexName, e);
    }
  }

  @Override
  public Map<String, FlintMetadata> getAllIndexMetadata(String... indexNamePattern) {
    LOG.info("Fetching all Flint index metadata for pattern " + String.join(",", indexNamePattern));
    String[] indexNames =
        Arrays.stream(indexNamePattern).map(this::sanitizeIndexName).toArray(String[]::new);
    try (IRestHighLevelClient client = createClient()) {
      GetIndexRequest request = new GetIndexRequest(indexNames);
      GetIndexResponse response = client.getIndex(request, RequestOptions.DEFAULT);

      return Arrays.stream(response.getIndices())
          .collect(Collectors.toMap(
              index -> index,
              index -> FlintMetadata.apply(
                  response.getMappings().get(index).source().toString(),
                  response.getSettings().get(index).toString()
              )
          ));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get Flint index metadata for " +
          String.join(",", indexNames), e);
    }
  }

  @Override
  public FlintMetadata getIndexMetadata(String indexName) {
    LOG.info("Fetching Flint index metadata for " + indexName);
    String osIndexName = sanitizeIndexName(indexName);
    try (IRestHighLevelClient client = createClient()) {
      GetIndexRequest request = new GetIndexRequest(osIndexName);
      GetIndexResponse response = client.getIndex(request, RequestOptions.DEFAULT);

      MappingMetadata mapping = response.getMappings().get(osIndexName);
      Settings settings = response.getSettings().get(osIndexName);
      return FlintMetadata.apply(mapping.source().string(), settings.toString());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get Flint index metadata for " + osIndexName, e);
    }
  }

  @Override
  public void updateIndex(String indexName, FlintMetadata metadata) {
    LOG.info("Updating Flint index " + indexName + " with metadata " + metadata);
    String osIndexName = sanitizeIndexName(indexName);
    try (IRestHighLevelClient client = createClient()) {
      PutMappingRequest request = new PutMappingRequest(osIndexName);
      request.source(metadata.getContent(), XContentType.JSON);
      client.updateIndexMapping(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to update Flint index " + osIndexName, e);
    }
  }

  @Override
  public void deleteIndex(String indexName) {
    LOG.info("Deleting Flint index " + indexName);
    String osIndexName = sanitizeIndexName(indexName);
    try (IRestHighLevelClient client = createClient()) {
      DeleteIndexRequest request = new DeleteIndexRequest(osIndexName);
      client.deleteIndex(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to delete Flint index " + osIndexName, e);
    }
  }

  public FlintWriter createWriter(String indexName) {
    LOG.info(String.format("Creating Flint index writer for %s, refresh_policy:%s, " +
        "batch_bytes:%d", indexName, options.getRefreshPolicy(), options.getBatchBytes()));
    return new OpenSearchWriter(createClient(), sanitizeIndexName(indexName),
        options.getRefreshPolicy(), options.getBatchBytes());
  }

  @Override
  public IRestHighLevelClient createClient() {
    return OpenSearchClientUtils.createClient(options);
  }

  /*
   * Because OpenSearch requires all lowercase letters in index name, we have to
   * lowercase all letters in the given Flint index name.
   */
  private String toLowercase(String indexName) {
    Objects.requireNonNull(indexName);

    return indexName.toLowerCase(Locale.ROOT);
  }

  /*
   * Percent-encode invalid OpenSearch index name characters.
   */
  private String percentEncode(String indexName) {
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

  /*
   * Sanitize index name to comply with OpenSearch index name restrictions.
   */
  private String sanitizeIndexName(String indexName) {
    Objects.requireNonNull(indexName);

    String encoded = percentEncode(indexName);
    return toLowercase(encoded);
  }
}
