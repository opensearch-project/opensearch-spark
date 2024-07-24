/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.auth;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.protocol.HttpContext;
import org.jetbrains.annotations.TestOnly;
import org.opensearch.common.Strings;
import org.opensearch.flint.core.storage.OpenSearchClientUtils;
import software.amazon.awssdk.authcrt.signer.AwsCrtV4aSigner;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Intercepts HTTP requests to sign them for AWS authentication, adjusting the signing process
 * based on whether the request accesses metadata or not.
 */
public class ResourceBasedAWSRequestSigningApacheInterceptor implements HttpRequestInterceptor {

    private final String service;
    private final String metadataAccessIdentifier;
    final HttpRequestInterceptor primaryInterceptor;
    final HttpRequestInterceptor metadataAccessInterceptor;

    /**
     * Constructs an interceptor for AWS request signing with optional metadata access.
     *
     * @param service                             The AWS service name.
     * @param region                              The AWS region for signing.
     * @param primaryCredentialsProvider          The credentials provider for general access.
     * @param metadataAccessCredentialsProvider   The credentials provider for metadata access.
     * @param metadataAccessIdentifier            Identifier for operations requiring metadata access.
     */
    public ResourceBasedAWSRequestSigningApacheInterceptor(final String service,
                                                           final String region,
                                                           final AWSCredentialsProvider primaryCredentialsProvider,
                                                           final AWSCredentialsProvider metadataAccessCredentialsProvider,
                                                           final String metadataAccessIdentifier) {
        if (Strings.isNullOrEmpty(service)) {
            throw new IllegalArgumentException("Service name must not be null or empty.");
        }
        if (Strings.isNullOrEmpty(region)) {
            throw new IllegalArgumentException("Region must not be null or empty.");
        }
        this.service = service;
        this.metadataAccessIdentifier = metadataAccessIdentifier;
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(service);
        signer.setRegionName(region);
        this.primaryInterceptor = new AWSRequestSigningApacheInterceptor(service, signer, primaryCredentialsProvider);
        this.metadataAccessInterceptor = primaryCredentialsProvider.equals(metadataAccessCredentialsProvider)
                ? this.primaryInterceptor
                : new AWSRequestSigV4ASigningApacheInterceptor(service, region, AwsCrtV4aSigner.builder().build(), metadataAccessCredentialsProvider);
    }

    // Test constructor allowing injection of mock interceptors
    @TestOnly
    ResourceBasedAWSRequestSigningApacheInterceptor(final String service,
                                                    final HttpRequestInterceptor primaryInterceptor,
                                                    final HttpRequestInterceptor metadataAccessInterceptor,
                                                    final String metadataAccessIdentifier) {
        this.service = service == null ? "unknown" : service;
        this.primaryInterceptor = primaryInterceptor;
        this.metadataAccessInterceptor = metadataAccessInterceptor;
        this.metadataAccessIdentifier = metadataAccessIdentifier;
    }

    /**
     * Processes an HTTP request, signing it according to whether it requires metadata access.
     *
     * @param request The HTTP request to process.
     * @param context The context in which the HTTP request is being processed.
     * @throws HttpException If processing the HTTP request results in an exception.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
        String resourcePath = parseUriToPath(request);
        if (OpenSearchClientUtils.AOS_SIGV4_SERVICE_NAME.equals(this.service) && isMetadataAccess(resourcePath)) {
            metadataAccessInterceptor.process(request, context);
        } else {
            primaryInterceptor.process(request, context);
        }
    }

    /**
     * Extracts and returns the path component of a URI from an HTTP request.
     *
     * @param request The HTTP request from which to extract the URI path.
     * @return The path component of the URI.
     * @throws IOException If an error occurs parsing the URI.
     */
    private String parseUriToPath(HttpRequest request) throws IOException {
        try {
            URIBuilder uriBuilder = new URIBuilder(request.getRequestLine().getUri());
            return uriBuilder.build().getRawPath();
        } catch (URISyntaxException e) {
            throw new IOException("Invalid URI", e);
        }
    }

    /**
     * Determines whether the accessed resource requires metadata credentials.
     *
     * @param resourcePath The path of the resource being accessed.
     * @return true if the operation requires metadata access credentials, false otherwise.
     */
    private boolean isMetadataAccess(String resourcePath) {
        return !Strings.isNullOrEmpty(metadataAccessIdentifier) && resourcePath.contains(metadataAccessIdentifier);
    }
}
