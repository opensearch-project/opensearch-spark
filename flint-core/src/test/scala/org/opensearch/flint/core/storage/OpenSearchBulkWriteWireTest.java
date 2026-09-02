/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.HttpHost;
import org.junit.jupiter.api.Test;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.flint.core.IRestHighLevelClient;

/**
 * Wire-level regression test for the production OpenSearch 2.6 high-level-client path.
 *
 * <p>Unlike the constructor-level tests, this starts a local HTTP endpoint, lets the real
 * {@link RestHighLevelClient} deserialize a bulk failure, and passes that response through
 * {@link OpenSearchWriter}. It specifically protects the subtle 2.6 behavior where a server-side
 * {@code security_exception} is reconstructed as the generic {@code OpenSearchException} class and
 * its original type survives only in the structured message prefix.
 */
class OpenSearchBulkWriteWireTest {

  private static final String SECRET_REASON = "customer-secret-reason";

  @Test
  void realRestClientPreservesSafeSecurityClassificationWithoutTheFailureReason() throws Exception {
    String responseBody = "{"
        + "\"took\":1,"
        + "\"errors\":true,"
        + "\"items\":[{\"create\":{"
        + "\"_index\":\"customer-index\","
        + "\"_id\":\"doc-1\","
        + "\"status\":403,"
        + "\"error\":{"
        + "\"type\":\"security_exception\","
        + "\"reason\":\"OpenSearch exception [type=authorization_exception, reason="
        + SECRET_REASON + "]\""
        + "}}}]}";

    OpenSearchBulkWriteException error = executeWireResponse(responseBody);

    assertEquals(403, error.getStatusCode());
    assertEquals(java.util.List.of("security_exception"), error.getExceptionTypeNames());
    assertTrue(error.getMessage().contains("type=security_exception"), error.getMessage());
    assertFalse(error.getMessage().contains(SECRET_REASON), error.getMessage());
    assertFalse(error.getMessage().contains("customer-index"), error.getMessage());
  }

  @Test
  void realRestClientKeepsClusterBlock403DistinctFromAuthorization() throws Exception {
    String responseBody = "{"
        + "\"took\":1,"
        + "\"errors\":true,"
        + "\"items\":[{\"create\":{"
        + "\"_index\":\"customer-index\","
        + "\"_id\":\"doc-1\","
        + "\"status\":403,"
        + "\"error\":{"
        + "\"type\":\"cluster_block_exception\","
        + "\"reason\":\"index blocked by: [FORBIDDEN/8/index write (api)]\""
        + "}}}]}";

    OpenSearchBulkWriteException error = executeWireResponse(responseBody);

    assertEquals(403, error.getStatusCode());
    assertEquals(java.util.List.of("cluster_block_exception"), error.getExceptionTypeNames());
    assertTrue(error.getMessage().contains("type=cluster_block_exception"), error.getMessage());
    assertFalse(error.getMessage().contains("index blocked by"), error.getMessage());
    assertFalse(error.getMessage().contains("customer-index"), error.getMessage());
  }

  /**
   * Real-server shape: an OpenSearch 2.6 security plugin returns {@code security_exception} with a
   * plain reason (not the {@code authorization_exception}-wrapped form). The body below is the
   * verbatim item error captured from a live security-enabled OpenSearch 2.6.0 bulk response when a
   * read-only user attempts an index. It must still classify as a 403 access-denied write while the
   * reason -- which names the requesting principal -- is dropped.
   */
  @Test
  void realRestClientClassifiesPlainSecurityException403AsAccessDeniedWithoutThePrincipal()
      throws Exception {
    String principalReason =
        "no permissions for [indices:data/write/index, indices:data/write/bulk[s]] and "
            + "User [name=probeuser, backend_roles=[], requestedTenant=null]";
    String responseBody = "{"
        + "\"took\":3,"
        + "\"errors\":true,"
        + "\"items\":[{\"index\":{"
        + "\"_index\":\"customer-index\","
        + "\"_id\":\"doc-1\","
        + "\"status\":403,"
        + "\"error\":{"
        + "\"type\":\"security_exception\","
        + "\"reason\":\"" + principalReason + "\""
        + "}}}]}";

    OpenSearchBulkWriteException error = executeWireResponse(responseBody);

    assertEquals(403, error.getStatusCode());
    assertEquals(java.util.List.of("security_exception"), error.getExceptionTypeNames());
    assertTrue(error.getMessage().contains("type=security_exception"), error.getMessage());
    assertFalse(error.getMessage().contains("probeuser"), error.getMessage());
    assertFalse(error.getMessage().contains("no permissions"), error.getMessage());
    assertFalse(error.getMessage().contains("customer-index"), error.getMessage());
  }

  /**
   * Real-server shape: a {@code mapper_parsing_exception} (HTTP 400) whose reason echoes the
   * offending document field value, plus a nested {@code number_format_exception} that repeats it.
   * The body below is the verbatim item error captured from a live OpenSearch 2.6.0 bulk response
   * when a string is indexed into an integer field. It must classify as a non-authorization 400
   * write error while every copy of the document value is dropped; only the closed-vocabulary type
   * names survive.
   */
  @Test
  void realRestClientClassifiesMapperParsing400AndDropsTheDocumentValue() throws Exception {
    String documentValue = "MAPPARSE_CANARY_B2_not_a_number";
    String responseBody = "{"
        + "\"took\":12,"
        + "\"errors\":true,"
        + "\"items\":[{\"index\":{"
        + "\"_index\":\"customer-index\","
        + "\"_id\":\"doc-1\","
        + "\"status\":400,"
        + "\"error\":{"
        + "\"type\":\"mapper_parsing_exception\","
        + "\"reason\":\"failed to parse field [n] of type [integer] in document with id '1'. "
        + "Preview of field's value: '" + documentValue + "'\","
        + "\"caused_by\":{"
        + "\"type\":\"number_format_exception\","
        + "\"reason\":\"For input string: \\\"" + documentValue + "\\\"\""
        + "}}}}]}";

    OpenSearchBulkWriteException error = executeWireResponse(responseBody);

    assertEquals(400, error.getStatusCode());
    assertTrue(
        error.getExceptionTypeNames().contains("mapper_parsing_exception"),
        error.getExceptionTypeNames().toString());
    assertTrue(error.getMessage().contains("type=mapper_parsing_exception"), error.getMessage());
    assertFalse(error.getMessage().contains(documentValue), error.getMessage());
    assertFalse(error.getMessage().contains("Preview of field"), error.getMessage());
    assertFalse(error.getMessage().contains("customer-index"), error.getMessage());
  }

  private static OpenSearchBulkWriteException executeWireResponse(String responseBody)
      throws Exception {
    try (SingleResponseServer server = new SingleResponseServer(responseBody);
         RestHighLevelClient realClient = new RestHighLevelClient(
             RestClient.builder(new HttpHost("127.0.0.1", server.port(), "http")))) {
      IRestHighLevelClient client = mock(IRestHighLevelClient.class);
      when(client.bulk(any(BulkRequest.class), any(RequestOptions.class)))
          .thenAnswer(invocation -> realClient.bulk(
              invocation.getArgument(0), invocation.getArgument(1)));

      OpenSearchWriter writer = new OpenSearchWriter(client, "customer-index", "false", 1024);
      writer.write("{\"create\":{\"_id\":\"doc-1\"}}\n{\"value\":1}\n");

      OpenSearchBulkWriteException error =
          assertThrows(OpenSearchBulkWriteException.class, writer::flush);

      server.assertCompleted();
      assertTrue(server.requestHeaders().startsWith("POST /_bulk"),
          server.requestHeaders());
      return error;
    }
  }

  /** One-shot loopback HTTP server that returns a supplied JSON response. */
  private static final class SingleResponseServer implements AutoCloseable {
    private static final byte[] HEADER_TERMINATOR = "\r\n\r\n".getBytes(UTF_8);

    private final ServerSocket serverSocket;
    private final Thread thread;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicReference<String> requestHeaders = new AtomicReference<>("");

    private SingleResponseServer(String body) throws IOException {
      serverSocket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
      thread = new Thread(() -> serve(body), "bulk-wire-test-server");
      thread.setDaemon(true);
      thread.start();
    }

    private int port() {
      return serverSocket.getLocalPort();
    }

    private String requestHeaders() {
      return requestHeaders.get();
    }

    private void serve(String body) {
      try (Socket socket = serverSocket.accept()) {
        InputStream input = socket.getInputStream();
        String headers = readHeaders(input);
        requestHeaders.set(headers);
        consumeRequestBody(input, contentLength(headers));
        byte[] bodyBytes = body.getBytes(UTF_8);
        String responseHeaders = "HTTP/1.1 200 OK\r\n"
            + "Content-Type: application/json\r\n"
            + "Content-Length: " + bodyBytes.length + "\r\n"
            + "Connection: close\r\n\r\n";
        OutputStream output = socket.getOutputStream();
        output.write(responseHeaders.getBytes(UTF_8));
        output.write(bodyBytes);
        output.flush();
      } catch (Throwable t) {
        failure.set(t);
      }
    }

    private static int contentLength(String headers) {
      for (String line : headers.split("\\r\\n")) {
        if (line.regionMatches(true, 0, "Content-Length:", 0, "Content-Length:".length())) {
          return Integer.parseInt(line.substring("Content-Length:".length()).trim());
        }
      }
      return 0;
    }

    private static void consumeRequestBody(InputStream input, int contentLength)
        throws IOException {
      int remaining = contentLength;
      byte[] buffer = new byte[Math.min(Math.max(contentLength, 1), 4096)];
      while (remaining > 0) {
        int read = input.read(buffer, 0, Math.min(buffer.length, remaining));
        if (read < 0) {
          throw new IOException("Connection closed before request body completed");
        }
        remaining -= read;
      }
    }

    private static String readHeaders(InputStream input) throws IOException {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      int matched = 0;
      while (matched < HEADER_TERMINATOR.length) {
        int value = input.read();
        if (value < 0) {
          throw new IOException("Connection closed before request headers completed");
        }
        bytes.write(value);
        matched = value == HEADER_TERMINATOR[matched] ? matched + 1
            : (value == HEADER_TERMINATOR[0] ? 1 : 0);
      }
      return bytes.toString(UTF_8.name());
    }

    private void assertCompleted() throws InterruptedException {
      thread.join(5000);
      assertFalse(thread.isAlive(), "Wire-test server did not complete");
      if (failure.get() != null) {
        throw new AssertionError("Wire-test server failed", failure.get());
      }
    }

    @Override
    public void close() throws Exception {
      serverSocket.close();
      thread.join(5000);
      if (failure.get() != null && !(failure.get() instanceof java.net.SocketException)) {
        throw new AssertionError("Wire-test server failed", failure.get());
      }
    }
  }
}
