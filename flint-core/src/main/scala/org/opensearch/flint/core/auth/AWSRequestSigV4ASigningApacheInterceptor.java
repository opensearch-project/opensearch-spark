/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.auth;

import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.glue.model.InvalidStateException;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.signer.AwsSignerExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;
import static org.opensearch.flint.core.auth.AWSRequestSigningApacheInterceptor.nvpToMapParams;
import static org.opensearch.flint.core.auth.AWSRequestSigningApacheInterceptor.skipHeader;

/**
 * Interceptor for signing AWS requests according to Signature Version 4A.
 * This interceptor processes HTTP requests, signs them with AWS credentials,
 * and updates the request headers to include the signature.
 */
public class AWSRequestSigV4ASigningApacheInterceptor implements HttpRequestInterceptor {
    private static final String HTTPS_PROTOCOL = "https";
    private static final int HTTPS_PORT = 443;

    private final String service;
    private final String region;
    private final Signer signer;
    private final AWSCredentialsProvider awsCredentialsProvider;

    /**
     * Constructs an interceptor for AWS request signing with metadata access.
     *
     * @param service                The AWS service name.
     * @param region                 The AWS region for signing.
     * @param signer                 The signer implementation.
     * @param awsCredentialsProvider The credentials provider for metadata access.
     */
    public AWSRequestSigV4ASigningApacheInterceptor(String service, String region, Signer signer, AWSCredentialsProvider awsCredentialsProvider) {
        this.service = service;
        this.region = region;
        this.signer = signer;
        this.awsCredentialsProvider = awsCredentialsProvider;
    }

    /**
     * Processes and signs an HTTP request, updating its headers with the signature.
     *
     * @param request the HTTP request to process and sign.
     * @param context the HTTP context associated with the request.
     * @throws IOException if an I/O error occurs during request processing.
     */
    @Override
    public void process(HttpRequest request, HttpContext context) throws IOException {
        SdkHttpFullRequest requestToSign = buildSdkHttpRequest(request, context);
        SdkHttpFullRequest signedRequest = signRequest(requestToSign);
        updateRequestHeaders(request, signedRequest.headers());
        updateRequestEntity(request, signedRequest);
    }

    /**
     * Builds an {@link SdkHttpFullRequest} from the Apache {@link HttpRequest}.
     *
     * @param request the HTTP request to process and sign.
     * @param context the HTTP context associated with the request.
     * @return an SDK HTTP request ready to be signed.
     * @throws IOException if an error occurs while building the request.
     */
    private SdkHttpFullRequest buildSdkHttpRequest(HttpRequest request, HttpContext context) throws IOException {
        URIBuilder uriBuilder = parseUri(request);
        SdkHttpFullRequest.Builder builder = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.fromValue(request.getRequestLine().getMethod()))
                .protocol(HTTPS_PROTOCOL)
                .port(HTTPS_PORT)
                .headers(headerArrayToMap(request.getAllHeaders()))
                .rawQueryParameters(nvpToMapParams(uriBuilder.getQueryParams()));

        HttpHost host = (HttpHost) context.getAttribute(HTTP_TARGET_HOST);
        if (host == null) {
            throw new InvalidStateException("Host must not be null");
        }
        builder.host(host.getHostName());
        try {
            builder.encodedPath(uriBuilder.build().getRawPath());
        } catch (URISyntaxException e) {
            throw new IOException("Invalid URI", e);
        }
        setRequestEntity(request, builder);
        return builder.build();
    }

    /**
     * Sets the request entity for the {@link SdkHttpFullRequest.Builder} if the original request contains an entity.
     * This is used for requests that have a body, such as POST or PUT requests.
     *
     * @param request the original HTTP request.
     * @param builder the SDK HTTP request builder.
     */
    private void setRequestEntity(HttpRequest request, SdkHttpFullRequest.Builder builder) {
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
            if (entity != null) {
                builder.contentStreamProvider(() -> {
                    try {
                        return entity.getContent();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
        }
    }

    private URIBuilder parseUri(HttpRequest request) throws IOException {
        try {
            return new URIBuilder(request.getRequestLine().getUri());
        } catch (URISyntaxException e) {
            throw new IOException("Invalid URI", e);
        }
    }

    /**
     * Signs the given SDK HTTP request using the provided AWS credentials and signer.
     *
     * @param request the SDK HTTP request to sign.
     * @return a signed SDK HTTP request.
     */
    private SdkHttpFullRequest signRequest(SdkHttpFullRequest request) {
        AWSSessionCredentials sessionCredentials = (AWSSessionCredentials) awsCredentialsProvider.getCredentials();
        AwsSessionCredentials awsCredentials = AwsSessionCredentials.create(
                sessionCredentials.getAWSAccessKeyId(),
                sessionCredentials.getAWSSecretKey(),
                sessionCredentials.getSessionToken()
        );

        ExecutionAttributes executionAttributes = new ExecutionAttributes()
                .putAttribute(AwsSignerExecutionAttribute.AWS_CREDENTIALS, awsCredentials)
                .putAttribute(AwsSignerExecutionAttribute.SERVICE_SIGNING_NAME, service)
                .putAttribute(AwsSignerExecutionAttribute.SIGNING_REGION, Region.of(region));

        return signer.sign(request, executionAttributes);
    }

    /**
     * Updates the HTTP request headers with the signed headers.
     *
     * @param request       the original HTTP request.
     * @param signedHeaders the headers after signing.
     */
    private void updateRequestHeaders(HttpRequest request, Map<String, List<String>> signedHeaders) {
        Header[] headers = convertHeaderMapToArray(signedHeaders);
        request.setHeaders(headers);
    }

    /**
     * Updates the request entity based on the signed request. This is used to update the request body after signing.
     *
     * @param request      the original HTTP request.
     * @param signedRequest the signed SDK HTTP request.
     */
    private void updateRequestEntity(HttpRequest request, SdkHttpFullRequest signedRequest) {
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntityEnclosingRequest httpEntityEnclosingRequest = (HttpEntityEnclosingRequest) request;
            signedRequest.contentStreamProvider().ifPresent(provider -> {
                InputStream contentStream = provider.newStream();
                BasicHttpEntity basicHttpEntity = new BasicHttpEntity();
                basicHttpEntity.setContent(contentStream);
                signedRequest.firstMatchingHeader("Content-Length").ifPresent(value ->
                        basicHttpEntity.setContentLength(Long.parseLong(value)));
                signedRequest.firstMatchingHeader("Content-Type").ifPresent(basicHttpEntity::setContentType);
                httpEntityEnclosingRequest.setEntity(basicHttpEntity);
            });
        }
    }

    /**
     * Converts an array of {@link Header} objects into a map, consolidating multiple values for the same header name.
     *
     * @param headers the array of {@link Header} objects to convert.
     * @return a map where each key is a header name and each value is a list of header values.
     */
    private static Map<String, List<String>> headerArrayToMap(final Header[] headers) {
        Map<String, List<String>> headersMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Header header : headers) {
            if (!skipHeader(header)) {
                headersMap.computeIfAbsent(header.getName(), k -> new ArrayList<>()).add(header.getValue());
            }
        }
        return headersMap;
    }

    /**
     * Converts a map of headers back into an array of {@link Header} objects.
     *
     * @param mapHeaders the map of headers to convert.
     * @return an array of {@link Header} objects.
     */
    private Header[] convertHeaderMapToArray(final Map<String, List<String>> mapHeaders) {
        return mapHeaders.entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), String.join(",", entry.getValue())))
                .toArray(Header[]::new);
    }
}