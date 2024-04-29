/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.auth;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;

import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class AWSRequestSigV4ASigningApacheInterceptorTest {
    @Mock
    private Signer mockSigner;
    @Mock
    private AWSCredentialsProvider mockCredentialsProvider;
    @Mock
    private HttpContext mockContext;
    @Mock
    private AWSSessionCredentials mockSessionCredentials;
    @Mock
    private SdkHttpFullRequest mockSdkHttpFullRequest;
    @Captor
    private ArgumentCaptor<SdkHttpFullRequest> sdkHttpFullRequestCaptor;

    private AWSRequestSigV4ASigningApacheInterceptor interceptor;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(mockCredentialsProvider.getCredentials()).thenReturn(mockSessionCredentials);
        when(mockSessionCredentials.getAWSAccessKeyId()).thenReturn("ACCESS_KEY_ID");
        when(mockSessionCredentials.getAWSSecretKey()).thenReturn("SECRET_ACCESS_KEY");
        when(mockSessionCredentials.getSessionToken()).thenReturn("SESSION_TOKEN");
        interceptor = new AWSRequestSigV4ASigningApacheInterceptor("s3", "us-west-2", mockSigner, mockCredentialsProvider);
        when(mockContext.getAttribute(HTTP_TARGET_HOST)).thenReturn(new HttpHost("localhost", 443, "https"));
        when(mockSigner.sign(any(), any())).thenReturn(mockSdkHttpFullRequest);
    }

    @Test
    public void testSigningProcess() throws Exception {
        HttpRequest request = new BasicHttpRequest("GET", "/path/to/resource");
        interceptor.process(request, mockContext);

        verify(mockSigner).sign(sdkHttpFullRequestCaptor.capture(), any());
        SdkHttpFullRequest signedRequest = sdkHttpFullRequestCaptor.getValue();

        assertEquals(SdkHttpMethod.GET, signedRequest.method());
        assertEquals("/path/to/resource", signedRequest.encodedPath());
    }

    @Test(expected = IOException.class)
    public void testInvalidUriHandling() throws Exception {
        HttpRequest request = new BasicHttpRequest("GET", ":///this/is/not/a/valid/uri");
        interceptor.process(request, mockContext);
    }

    @Test
    public void testHeaderUpdateAfterSigning() throws Exception {
        // Setup mock signer to return a new SdkHttpFullRequest with an "Authorization" header
        when(mockSigner.sign(any(SdkHttpFullRequest.class), any())).thenAnswer(invocation -> {
            SdkHttpFullRequest originalRequest = invocation.getArgument(0);
            Map<String, List<String>> modifiedHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            modifiedHeaders.putAll(originalRequest.headers());
            modifiedHeaders.put("Authorization", List.of("AWS4-ECDSA-P256-SHA256 Credential=..."));

            // Build a new SdkHttpFullRequest with the modified headers
            return SdkHttpFullRequest.builder()
                    .method(originalRequest.method())
                    .uri(originalRequest.getUri())
                    .headers(modifiedHeaders)
                    .build();
        });

        HttpRequest request = new BasicHttpRequest("GET", "/path/to/resource");
        interceptor.process(request, mockContext);

        // Now verify that the HttpRequest has been updated with the new headers from the signed request
        assertTrue("The request does not contain the expected 'Authorization' header",
                request.containsHeader("Authorization"));
        assertEquals("AWS4-ECDSA-P256-SHA256 Credential=...",
                request.getFirstHeader("Authorization").getValue());
    }

    @Test
    public void testSigningProcessWithCorrectHostFormat() throws Exception {
        HttpRequest request = new BasicHttpRequest("GET", "/path/to/resource");

        // Setup the interceptor with a mock HTTP context to return an HttpHost with scheme and port
        HttpHost expectedHost = new HttpHost("localhost", 443, "https");
        when(mockContext.getAttribute(HTTP_TARGET_HOST)).thenReturn(expectedHost);

        interceptor.process(request, mockContext);

        // Capture the SdkHttpFullRequest passed to the signer
        verify(mockSigner).sign(sdkHttpFullRequestCaptor.capture(), any());
        SdkHttpFullRequest signedRequest = sdkHttpFullRequestCaptor.getValue();

        // Assert method and path
        assertEquals(SdkHttpMethod.GET, signedRequest.method());
        assertEquals("/path/to/resource", signedRequest.encodedPath());

        // Verify the host format is correct (hostname only, without scheme and port)
        String expectedHostName = "localhost"; // Expected hostname without scheme and port
        assertEquals("The host in the signed request should contain only the hostname, without scheme and port.",
                expectedHostName, signedRequest.host());
    }

    @Test
    public void testConnectionHeaderIsSkippedDuringSigning() throws Exception {
        // Create a new HttpRequest with a Connection header
        HttpRequest request = new BasicHttpRequest("GET", "/path/to/resource");
        request.addHeader("Connection", "keep-alive");

        interceptor.process(request, mockContext);

        // Verify that the SdkHttpFullRequest passed to the signer does not contain the Connection header
        verify(mockSigner).sign(sdkHttpFullRequestCaptor.capture(), any());
        SdkHttpFullRequest signedRequest = sdkHttpFullRequestCaptor.getValue();

        // Assert that the signed request does not have the Connection header
        assertFalse("The signed request should not contain a 'Connection' header.",
                signedRequest.headers().containsKey("Connection"));
    }
}