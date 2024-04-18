package org.opensearch.flint.core.auth;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.Signer;
import org.apache.http.HttpRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

public class ResourceBasedAWSRequestSigningApacheInterceptorTest {

    @Mock
    private Signer mockSigner;
    @Mock
    private AWSCredentialsProvider mockPrimaryCredentialsProvider;
    @Mock
    private AWSCredentialsProvider mockMetadataAccessCredentialsProvider;
    @Mock
    private HttpContext mockContext;
    @Captor
    private ArgumentCaptor<HttpRequest> httpRequestCaptor;

    private ResourceBasedAWSRequestSigningApacheInterceptor interceptor;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        AWSRequestSigningApacheInterceptor primaryInterceptorSpy = spy(new AWSRequestSigningApacheInterceptor("es", mockSigner, mockPrimaryCredentialsProvider));
        AWSRequestSigningApacheInterceptor metadataInterceptorSpy = spy(new AWSRequestSigningApacheInterceptor("es", mockSigner, mockMetadataAccessCredentialsProvider));

        interceptor = new ResourceBasedAWSRequestSigningApacheInterceptor(
                "es",
                primaryInterceptorSpy,
                metadataInterceptorSpy,
                "/metadata");
    }

    @Test
    public void testProcessWithMetadataAccess() throws Exception {
        HttpRequest request = new BasicHttpRequest("GET", "/es/metadata/resource");

        interceptor.process(request, mockContext);

        verify(interceptor.metadataAccessInterceptor).process(httpRequestCaptor.capture(), eq(mockContext));
        verify(interceptor.primaryInterceptor, never()).process(any(HttpRequest.class), any(HttpContext.class));
        assert httpRequestCaptor.getValue().getRequestLine().getUri().contains("/metadata");
    }

    @Test
    public void testProcessWithoutMetadataAccess() throws Exception {
        HttpRequest request = new BasicHttpRequest("GET", "/es/regular/resource");

        interceptor.process(request, mockContext);

        verify(interceptor.primaryInterceptor).process(httpRequestCaptor.capture(), eq(mockContext));
        verify(interceptor.metadataAccessInterceptor, never()).process(any(HttpRequest.class), any(HttpContext.class));
        assert !httpRequestCaptor.getValue().getRequestLine().getUri().contains("/metadata");
    }
}
