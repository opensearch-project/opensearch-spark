/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.logging;

import static org.junit.Assert.assertEquals;
import static org.opensearch.flint.core.logging.ExceptionMessages.extractRootCause;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.spark.SparkException;
import org.junit.Test;
import org.opensearch.OpenSearchException;

public class ExceptionMessagesTest {

  @Test
  public void testExtractRootCauseFromS3Exception() {
    AmazonS3Exception exception = new AmazonS3Exception("test");
    exception.setServiceName("S3");
    exception.setStatusCode(400);

    assertEquals(
        "Fail to read data from S3. Cause: serviceName=[S3], statusCode=[400]",
        extractRootCause(
            new SparkException("test", exception)));
  }

  @Test
  public void testExtractRootCauseFromOtherException() {
    OpenSearchException exception = new OpenSearchException("Write is blocked");

    assertEquals(
        "Write is blocked",
        extractRootCause(
            new SparkException("test", exception)));
  }

  @Test
  public void testExtractRootCauseFromOtherExceptionWithLongMessage() {
    OpenSearchException exception = new OpenSearchException("Write is blocked due to cluster is readonly");

    assertEquals(
        "Write is blocked due...",
        extractRootCause(
            new SparkException("test", exception), 20));
  }
}