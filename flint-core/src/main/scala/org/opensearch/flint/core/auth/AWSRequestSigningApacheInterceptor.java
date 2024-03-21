/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.auth;

import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;

import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.Signer;
import com.amazonaws.http.HttpMethodName;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;

/**
 * From https://github.com/opensearch-project/sql-jdbc/blob/main/src/main/java/org/opensearch/jdbc/transport/http/auth/aws/AWSRequestSigningApacheInterceptor.java
 * An {@link HttpRequestInterceptor} that signs requests using any AWS {@link Signer} for SIGV4_AUTH
 * and {@link AWSCredentialsProvider}.
 */
public class AWSRequestSigningApacheInterceptor implements HttpRequestInterceptor {

  /**
   * The service that we're connecting to. Technically not necessary.
   * Could be used by a future Signer, though.
   */
  private final String service;

  /**
   * The particular signer implementation.
   */
  private final Signer signer;

  /**
   * Provides the primary source of AWS credentials used for signing requests. These credentials are used
   * for the majority of requests, except in cases where elevated permissions are required.
   */
  private final AWSCredentialsProvider primaryCredentialsProvider;

  /**
   * Provides a source of AWS credentials that are used for signing requests requiring elevated permissions.
   * This is particularly useful for accessing resources that are restricted to super-administrative operations,
   * such as certain system indices or administrative APIs. These credentials are expected to have permissions
   * beyond those of the regular {@link #primaryCredentialsProvider}.
   */
  private final AWSCredentialsProvider superAdminAWSCredentialsProvider;

  /**
   * Identifies data access operations that require super-admin credentials. This identifier can be used to
   * distinguish between regular and elevated data access needs, facilitating the decision to use
   * {@link #superAdminAWSCredentialsProvider} over {@link #primaryCredentialsProvider} when accessing sensitive
   * or restricted resources.
   */
  private final String superAdminDataAccessIdentifier;

  /**
   *
   * @param service service that we're connecting to
   * @param signer particular signer implementation
   * @param primaryCredentialsProvider source of AWS credentials for signing
   */
  public AWSRequestSigningApacheInterceptor(final String service,
                                            final Signer signer,
                                            final AWSCredentialsProvider primaryCredentialsProvider,
                                            final AWSCredentialsProvider superAdminAWSCredentialsProvider,
                                            final String superAdminDataAccessIdentifier) {
    this.service = service;
    this.signer = signer;
    this.primaryCredentialsProvider = primaryCredentialsProvider;
    this.superAdminAWSCredentialsProvider = superAdminAWSCredentialsProvider;
    this.superAdminDataAccessIdentifier = superAdminDataAccessIdentifier;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void process(final HttpRequest request, final HttpContext context)
      throws HttpException, IOException {
    URIBuilder uriBuilder;
    try {
      uriBuilder = new URIBuilder(request.getRequestLine().getUri());
    } catch (URISyntaxException e) {
      throw new IOException("Invalid URI" , e);
    }

    // Copy Apache HttpRequest to AWS DefaultRequest
    DefaultRequest<?> signableRequest = new DefaultRequest<>(service);

    HttpHost host = (HttpHost) context.getAttribute(HTTP_TARGET_HOST);
    if (host != null) {
      signableRequest.setEndpoint(URI.create(host.toURI()));
    }
    final HttpMethodName httpMethod =
        HttpMethodName.fromValue(request.getRequestLine().getMethod());
    signableRequest.setHttpMethod(httpMethod);
    try {
      signableRequest.setResourcePath(uriBuilder.build().getRawPath());
    } catch (URISyntaxException e) {
      throw new IOException("Invalid URI" , e);
    }

    if (request instanceof HttpEntityEnclosingRequest) {
      HttpEntityEnclosingRequest httpEntityEnclosingRequest =
          (HttpEntityEnclosingRequest) request;
      if (httpEntityEnclosingRequest.getEntity() != null) {
        signableRequest.setContent(httpEntityEnclosingRequest.getEntity().getContent());
      }
    }
    signableRequest.setParameters(nvpToMapParams(uriBuilder.getQueryParams()));
    signableRequest.setHeaders(headerArrayToMap(request.getAllHeaders()));

    // Sign it
    if (this.service.equals("es") && isSuperAdminDataAccess(signableRequest.getResourcePath())) {
      signer.sign(signableRequest, superAdminAWSCredentialsProvider.getCredentials());
    } else {
      signer.sign(signableRequest, primaryCredentialsProvider.getCredentials());
    }

    // Now copy everything back
    request.setHeaders(mapToHeaderArray(signableRequest.getHeaders()));
    if (request instanceof HttpEntityEnclosingRequest) {
      HttpEntityEnclosingRequest httpEntityEnclosingRequest =
          (HttpEntityEnclosingRequest) request;
      if (httpEntityEnclosingRequest.getEntity() != null) {
        BasicHttpEntity basicHttpEntity = new BasicHttpEntity();
        basicHttpEntity.setContent(signableRequest.getContent());
        httpEntityEnclosingRequest.setEntity(basicHttpEntity);
      }
    }
  }

  /**
   *
   * @param params list of HTTP query params as NameValuePairs
   * @return a multimap of HTTP query params
   */
  private static Map<String, List<String>> nvpToMapParams(final List<NameValuePair> params) {
    Map<String, List<String>> parameterMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (NameValuePair nvp : params) {
      List<String> argsList =
          parameterMap.computeIfAbsent(nvp.getName(), k -> new ArrayList<>());
      argsList.add(nvp.getValue());
    }
    return parameterMap;
  }

  /**
   * @param resourcePath The path of the resource being accessed.
   * @return true if the resource path contains the super-admin data access identifier, indicating that
   * the operation requires super-admin credentials; false otherwise.
   */
  private boolean isSuperAdminDataAccess(String resourcePath) {
    return resourcePath.contains(superAdminDataAccessIdentifier);
  }

  /**
   * @param headers modeled Header objects
   * @return a Map of header entries
   */
  private static Map<String, String> headerArrayToMap(final Header[] headers) {
    Map<String, String> headersMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (Header header : headers) {
      if (!skipHeader(header)) {
        headersMap.put(header.getName(), header.getValue());
      }
    }
    return headersMap;
  }

  /**
   * @param header header line to check
   * @return true if the given header should be excluded when signing
   */
  private static boolean skipHeader(final Header header) {
    return ("content-length".equalsIgnoreCase(header.getName())
        && "0".equals(header.getValue())) // Strip Content-Length: 0
        || "host".equalsIgnoreCase(header.getName()); // Host comes from endpoint
  }

  /**
   * @param mapHeaders Map of header entries
   * @return modeled Header objects
   */
  private static Header[] mapToHeaderArray(final Map<String, String> mapHeaders) {
    Header[] headers = new Header[mapHeaders.size()];
    int i = 0;
    for (Map.Entry<String, String> headerEntry : mapHeaders.entrySet()) {
      headers[i++] = new BasicHeader(headerEntry.getKey(), headerEntry.getValue());
    }
    return headers;
  }
}
