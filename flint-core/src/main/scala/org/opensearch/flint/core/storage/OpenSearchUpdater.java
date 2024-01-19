package org.opensearch.flint.core.storage;

import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.core.FlintClient;
import org.opensearch.flint.core.RestHighLevelClientWrapper;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OpenSearchUpdater {
    private static final Logger LOG = Logger.getLogger(OpenSearchUpdater.class.getName());

    private final String indexName;

    private final FlintClient flintClient;


    public OpenSearchUpdater(String indexName, FlintClient flintClient) {
        this.indexName = indexName;
        this.flintClient = flintClient;
    }

    public void upsert(String id, String doc) {
        // we might need to keep the updater for a long time. Reusing the client may not work as the temporary
        // credentials may expire.
        // also, failure to close the client causes the job to be stuck in the running state as the client resource
        // is not released.
        try (RestHighLevelClientWrapper client = flintClient.createClient()) {
            assertIndexExist(client, indexName);
            UpdateRequest
                    updateRequest =
                    new UpdateRequest(indexName, id).doc(doc, XContentType.JSON)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                            .docAsUpsert(true);
            client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "Failed to execute update request on index: %s, id: %s",
                    indexName,
                    id), e);
        }
    }

    public void update(String id, String doc) {
        try (RestHighLevelClientWrapper client = flintClient.createClient()) {
            assertIndexExist(client, indexName);
            UpdateRequest
                    updateRequest =
                    new UpdateRequest(indexName, id).doc(doc, XContentType.JSON)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "Failed to execute update request on index: %s, id: %s",
                    indexName,
                    id), e);
        }
    }

    public void updateIf(String id, String doc, long seqNo, long primaryTerm) {
        try (RestHighLevelClientWrapper client = flintClient.createClient()) {
            assertIndexExist(client, indexName);
            UpdateRequest
                    updateRequest =
                    new UpdateRequest(indexName, id).doc(doc, XContentType.JSON)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                            .setIfSeqNo(seqNo)
                            .setIfPrimaryTerm(primaryTerm);
            client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "Failed to execute update request on index: %s, id: %s",
                    indexName,
                    id), e);
        }
    }

    private void assertIndexExist(RestHighLevelClientWrapper client, String indexName) throws IOException {
        LOG.info("Checking if index exists " + indexName);
        if (!client.isIndexExists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
            String errorMsg = "Index not found " + indexName;
            LOG.log(Level.SEVERE, errorMsg);
            throw new IllegalStateException(errorMsg);
        }
    }
}
