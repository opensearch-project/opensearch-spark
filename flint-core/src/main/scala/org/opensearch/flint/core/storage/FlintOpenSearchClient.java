/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import static org.opensearch.common.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.core.FlintClient;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.flint.core.metadata.FlintMetadata;
import org.opensearch.flint.core.metadata.log.DefaultOptimisticTransaction;
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry;
import org.opensearch.flint.core.metadata.log.OptimisticTransaction;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import scala.Option;
import scala.Some;

/**
 * Flint client implementation for OpenSearch storage.
 */
public class FlintOpenSearchClient implements FlintClient {

  private static final Logger LOG = Logger.getLogger(FlintOpenSearchClient.class.getName());


  /**
   * {@link NamedXContentRegistry} from {@link SearchModule} used for construct {@link QueryBuilder} from DSL query string.
   */
  private final static NamedXContentRegistry
      xContentRegistry =
      new NamedXContentRegistry(new SearchModule(Settings.builder().build(),
          new ArrayList<>()).getNamedXContents());

  /**
   * Invalid index name characters to percent-encode,
   * excluding '*' because it's reserved for pattern matching.
   */
  private final static Set<Character> INVALID_INDEX_NAME_CHARS =
      Set.of(' ', ',', ':', '"', '+', '/', '\\', '|', '?', '#', '>', '<');

  /**
   * Metadata log index name prefix
   */
  public final static String META_LOG_NAME_PREFIX = ".query_execution_request";

  private final FlintOptions options;
  private final String dataSourceName;
  private final String metaLogIndexName;

  public FlintOpenSearchClient(FlintOptions options) {
    this.options = options;
    this.dataSourceName = options.getDataSourceName();
    this.metaLogIndexName = constructMetaLogIndexName();
  }

  @Override
  public <T> OptimisticTransaction<T> startTransaction(String indexName, boolean forceInit) {
    LOG.info("Starting transaction on index " + indexName + " and data source " + dataSourceName);
    try (IRestHighLevelClient client = createClient()) {
      if (client.doesIndexExist(new GetIndexRequest(metaLogIndexName), RequestOptions.DEFAULT)) {
        LOG.info("Found metadata log index " + metaLogIndexName);
      } else {
        if (forceInit) {
          createIndex(metaLogIndexName, FlintMetadataLogEntry.QUERY_EXECUTION_REQUEST_MAPPING(),
              Some.apply(FlintMetadataLogEntry.QUERY_EXECUTION_REQUEST_SETTINGS()));
        } else {
          String errorMsg = "Metadata log index not found " + metaLogIndexName;
          LOG.warning(errorMsg);
          throw new IllegalStateException(errorMsg);
        }
      }
      return new DefaultOptimisticTransaction<>(dataSourceName,
          new FlintOpenSearchMetadataLog(options, indexName, metaLogIndexName));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to check if index metadata log index exists " + metaLogIndexName, e);
    }
  }

  @Override
  public <T> OptimisticTransaction<T> startTransaction(String indexName) {
    return startTransaction(indexName, false);
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
  public List<FlintMetadata> getAllIndexMetadata(String indexNamePattern) {
    LOG.info("Fetching all Flint index metadata for pattern " + indexNamePattern);
    String osIndexNamePattern = sanitizeIndexName(indexNamePattern);
    try (IRestHighLevelClient client = createClient()) {
      GetIndexRequest request = new GetIndexRequest(osIndexNamePattern);
      GetIndexResponse response = client.getIndex(request, RequestOptions.DEFAULT);

      return Arrays.stream(response.getIndices())
          .map(index -> constructFlintMetadata(
              index,
              response.getMappings().get(index).source().toString(),
              response.getSettings().get(index).toString()))
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get Flint index metadata for " + osIndexNamePattern, e);
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
      return constructFlintMetadata(indexName, mapping.source().string(), settings.toString());
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

  /**
   * Create {@link FlintReader}.
   *
   * @param indexName index name.
   * @param query     DSL query. DSL query is null means match_all.
   * @return {@link FlintReader}.
   */
  @Override
  public FlintReader createReader(String indexName, String query) {
    LOG.info("Creating Flint index reader for " + indexName + " with query " + query);
    try {
      QueryBuilder queryBuilder = new MatchAllQueryBuilder();
      if (!Strings.isNullOrEmpty(query)) {
        XContentParser
            parser =
            XContentType.JSON.xContent().createParser(xContentRegistry, IGNORE_DEPRECATIONS, query);
        queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
      }
      return new OpenSearchScrollReader(createClient(),
          sanitizeIndexName(indexName),
          new SearchSourceBuilder().query(queryBuilder),
          options);
    } catch (IOException e) {
      throw new RuntimeException(e);
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
   * Constructs Flint metadata with latest metadata log entry attached if it's available.
   * It relies on FlintOptions to provide data source name.
   */
  private FlintMetadata constructFlintMetadata(String indexName, String mapping, String settings) {
    String dataSourceName = options.getDataSourceName();
    String metaLogIndexName = dataSourceName.isEmpty() ? META_LOG_NAME_PREFIX
        : META_LOG_NAME_PREFIX + "_" + dataSourceName;
    Optional<FlintMetadataLogEntry> latest = Optional.empty();

    try (IRestHighLevelClient client = createClient()) {
      if (client.doesIndexExist(new GetIndexRequest(metaLogIndexName), RequestOptions.DEFAULT)) {
        LOG.info("Found metadata log index " + metaLogIndexName);
        FlintOpenSearchMetadataLog metadataLog =
            new FlintOpenSearchMetadataLog(options, indexName, metaLogIndexName);
        latest = metadataLog.getLatest();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to check if index metadata log index exists " + metaLogIndexName, e);
    }

    if (latest.isEmpty()) {
      return FlintMetadata.apply(mapping, settings);
    } else {
      return FlintMetadata.apply(mapping, settings, latest.get());
    }
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

  private String constructMetaLogIndexName() {
    return dataSourceName.isEmpty() ? META_LOG_NAME_PREFIX : META_LOG_NAME_PREFIX + "_" + dataSourceName;
  }
}
