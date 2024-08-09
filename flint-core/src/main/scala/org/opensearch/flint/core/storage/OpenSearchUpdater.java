package org.opensearch.flint.core.storage;

import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.core.FlintClient;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.IRestHighLevelClient;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.opensearch.flint.core.metrics.MetricConstants.REQUEST_METADATA_READ_METRIC_PREFIX;
import static org.opensearch.flint.core.metrics.MetricConstants.REQUEST_METADATA_WRITE_METRIC_PREFIX;

/**
 * Provides functionality for updating and upserting documents in an OpenSearch index.
 * This class utilizes FlintClient for managing connections to OpenSearch and performs
 * document updates and upserts with optional optimistic concurrency control.
 */
public class OpenSearchUpdater {
    private static final Logger LOG = Logger.getLogger(OpenSearchUpdater.class.getName());

    private final String indexName;
    private final FlintClient flintClient;
    private final FlintOptions options;

    public OpenSearchUpdater(String indexName, FlintClient flintClient, FlintOptions options) {
        this.indexName = indexName;
        this.flintClient = flintClient;
        this.options = options;
    }

    public void upsert(String id, String doc) {
        updateDocument(id, doc, true, -1, -1);
    }

    public void update(String id, String doc) {
        updateDocument(id, doc, false, -1, -1);
    }

    public void updateIf(String id, String doc, long seqNo, long primaryTerm) {
        updateDocument(id, doc, false, seqNo, primaryTerm);
    }

    /**
     * Internal method for updating or upserting a document with optional optimistic concurrency control.
     *
     * @param id The document ID.
     * @param doc The document content in JSON format.
     * @param upsert Flag indicating whether to upsert the document.
     * @param seqNo The sequence number for optimistic concurrency control.
     * @param primaryTerm The primary term for optimistic concurrency control.
     */
    private void updateDocument(String id, String doc, boolean upsert, long seqNo, long primaryTerm) {
        // we might need to keep the updater for a long time. Reusing the client may not work as the temporary
        // credentials may expire.
        // also, failure to close the client causes the job to be stuck in the running state as the client resource
        // is not released.
        try (IRestHighLevelClient client = flintClient.createClient()) {
            assertIndexExist(client, indexName);
            UpdateRequest updateRequest = new UpdateRequest(indexName, id)
                    .doc(doc, XContentType.JSON)
                    .setRefreshPolicy(options.getRefreshPolicy());

            if (upsert) {
                updateRequest.docAsUpsert(true);
            }
            if (seqNo >= 0 && primaryTerm >= 0) {
                updateRequest.setIfSeqNo(seqNo).setIfPrimaryTerm(primaryTerm);
            }

            try {
                client.update(updateRequest, RequestOptions.DEFAULT);
                IRestHighLevelClient.recordOperationSuccess(REQUEST_METADATA_WRITE_METRIC_PREFIX);
            } catch (Exception e) {
                IRestHighLevelClient.recordOperationFailure(REQUEST_METADATA_WRITE_METRIC_PREFIX, e);
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "Failed to execute update request on index: %s, id: %s",
                    indexName, id), e);
        }
    }

    private void assertIndexExist(IRestHighLevelClient client, String indexName) throws IOException {
        LOG.info("Checking if index exists: " + indexName);
        boolean exists;
        try {
            exists = client.doesIndexExist(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
            IRestHighLevelClient.recordOperationSuccess(REQUEST_METADATA_READ_METRIC_PREFIX);
        } catch (Exception e) {
            IRestHighLevelClient.recordOperationFailure(REQUEST_METADATA_READ_METRIC_PREFIX, e);
            throw e;
        }

        if (!exists) {
            String errorMsg = "Index not found: " + indexName;
            LOG.log(Level.SEVERE, errorMsg);
            throw new IllegalStateException(errorMsg);
        }
    }
}

