package org.opensearch.flint.core.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.auth.signer.internal.SignerConstant.X_AMZ_CONTENT_SHA256;

import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.Signer;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.util.IOUtils;
import java.io.IOException;
import java.net.URI;
import org.apache.http.HttpHost;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.utils.StringInputStream;

@ExtendWith(MockitoExtension.class)
class AWSRequestSigningApacheInterceptorTest {

  @Mock
  AWSCredentialsProvider awsCredentialsProvider;
  @Mock Signer signer;
  @Mock
  AWSCredentials awsCredentials;

  @Captor
  ArgumentCaptor<DefaultRequest<?>> signableRequestCaptor;

  @Test
  public void testProcessWithServiceIsEs() throws Exception {
    AWSRequestSigningApacheInterceptor awsRequestSigningApacheInterceptor = new AWSRequestSigningApacheInterceptor("es", signer, awsCredentialsProvider);
    final BasicHttpEntityEnclosingRequest request = getRequestWithEntity();
    final BasicHttpContext context = getContext();
    when(awsCredentialsProvider.getCredentials()).thenReturn(awsCredentials);

    awsRequestSigningApacheInterceptor.process(request, context);

    verify(signer).sign(signableRequestCaptor.capture(), eq(awsCredentials));
    DefaultRequest<?> signableRequest = signableRequestCaptor.getValue();
    assertEquals(new URI("http://hello.world"), signableRequest.getEndpoint());
    assertEquals(HttpMethodName.POST, signableRequest.getHttpMethod());
    assertEquals("/path", signableRequest.getResourcePath());
    assertEquals("ENTITY", IOUtils.toString(signableRequest.getContent()));
    assertEquals("HeaderValue", signableRequest.getHeaders().get("Test-Header"));
    assertEquals("value0", signableRequest.getParameters().get("param0").get(0));
  }

  @Test
  public void testProcessWithoutEntity() throws Exception {
    AWSRequestSigningApacheInterceptor awsRequestSigningApacheInterceptor = new AWSRequestSigningApacheInterceptor("es", signer, awsCredentialsProvider);
    final BasicHttpEntityEnclosingRequest request = getRequest();
    final BasicHttpContext context = getContext();
    when(awsCredentialsProvider.getCredentials()).thenReturn(awsCredentials);

    awsRequestSigningApacheInterceptor.process(request, context);

    verify(signer).sign(signableRequestCaptor.capture(), eq(awsCredentials));
    DefaultRequest<?> signableRequest = signableRequestCaptor.getValue();
    assertEquals("", IOUtils.toString(signableRequest.getContent()));
  }

  @NotNull
  private static BasicHttpContext getContext() {
    BasicHttpContext context = new BasicHttpContext();
    context.setAttribute(HttpCoreContext.HTTP_TARGET_HOST, new HttpHost("hello.world"));
    return context;
  }

  @Test
  public void testProcessWithServiceIsAoss() throws Exception {
    AWSRequestSigningApacheInterceptor awsRequestSigningApacheInterceptor = new AWSRequestSigningApacheInterceptor("aoss", signer, awsCredentialsProvider);
    final BasicHttpEntityEnclosingRequest request = getRequest();
    final BasicHttpContext context = getContext();
    when(awsCredentialsProvider.getCredentials()).thenReturn(awsCredentials);

    awsRequestSigningApacheInterceptor.process(request, context);

    verify(signer).sign(signableRequestCaptor.capture(), eq(awsCredentials));
    DefaultRequest<?> signableRequest = signableRequestCaptor.getValue();
    assertEquals("required", signableRequest.getHeaders().get(X_AMZ_CONTENT_SHA256));
  }

  @Test
  public void testInvalidURI() throws Exception {
    AWSRequestSigningApacheInterceptor awsRequestSigningApacheInterceptor = new AWSRequestSigningApacheInterceptor("aoss", signer, awsCredentialsProvider);
    final BasicHttpEntityEnclosingRequest request = new BasicHttpEntityEnclosingRequest("POST", "::INVALID_URI::");
    final BasicHttpContext context = getContext();

    assertThrows(IOException.class, () -> {
        awsRequestSigningApacheInterceptor.process(request, context);
    });
  }

  @NotNull
  private static BasicHttpEntityEnclosingRequest getRequestWithEntity() {
    BasicHttpEntityEnclosingRequest request = getRequest();
    BasicHttpEntity basicHttpEntity = new BasicHttpEntity();
    basicHttpEntity.setContent(new StringInputStream("ENTITY"));
    request.setEntity(basicHttpEntity);
    request.setHeader("content-length", "6");
    return request;
  }

  @NotNull
  private static BasicHttpEntityEnclosingRequest getRequest() {
    BasicHttpEntityEnclosingRequest request = new BasicHttpEntityEnclosingRequest("POST", "https://hello.world/path?param0=value0");
    request.setHeader("Test-Header", "HeaderValue");
    request.setHeader("content-length", "0");
    return request;
  }
}