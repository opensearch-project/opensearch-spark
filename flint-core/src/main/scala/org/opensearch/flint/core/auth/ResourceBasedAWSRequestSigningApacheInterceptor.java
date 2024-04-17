package org.opensearch.flint.core.auth;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.Signer;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Intercepts HTTP requests to sign them for AWS authentication, adjusting the signing process
 * based on whether the request accesses metadata or not.
 */
public class ResourceBasedAWSRequestSigningApacheInterceptor implements HttpRequestInterceptor {

    private final String service;
    private final String metadataAccessIdentifier;
    final AWSRequestSigningApacheInterceptor primaryInterceptor;
    final AWSRequestSigningApacheInterceptor metadataAccessInterceptor;

    /**
     * Constructs an interceptor for AWS request signing with optional metadata access.
     *
     * @param service                             The AWS service name.
     * @param signer                              The AWS request signer.
     * @param primaryCredentialsProvider          The credentials provider for general access.
     * @param metadataAccessCredentialsProvider   The credentials provider for metadata access.
     * @param metadataAccessIdentifier            Identifier for operations requiring metadata access.
     */
    public ResourceBasedAWSRequestSigningApacheInterceptor(final String service,
                                                           final Signer signer,
                                                           final AWSCredentialsProvider primaryCredentialsProvider,
                                                           final AWSCredentialsProvider metadataAccessCredentialsProvider,
                                                           final String metadataAccessIdentifier) {
        this(service,
                new AWSRequestSigningApacheInterceptor(service, signer, primaryCredentialsProvider),
                new AWSRequestSigningApacheInterceptor(service, signer, metadataAccessCredentialsProvider),
                metadataAccessIdentifier);
    }

    // Test constructor allowing injection of mock interceptors
    ResourceBasedAWSRequestSigningApacheInterceptor(final String service,
                                                    final AWSRequestSigningApacheInterceptor primaryInterceptor,
                                                    final AWSRequestSigningApacheInterceptor metadataAccessInterceptor,
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
        if ("es".equals(this.service) && isMetadataAccess(resourcePath)) {
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
        return resourcePath.contains(metadataAccessIdentifier);
    }
}
