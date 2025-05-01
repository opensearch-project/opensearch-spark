/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.spark.e2e

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

/**
 * Provides a method for obtaining an S3 client.
 */
trait S3ClientTrait {
  var s3Client : AmazonS3 = null

  /**
   * Retrieves the S3 access key to use
   *
   * @return S3 access key to use
   */
  def getS3AccessKey(): String

  /**
   * Retrieves the S3 secret key to use
   *
   * @return S3 secret key to use
   */
  def getS3SecretKey(): String

  /**
   * Returns an S3 client. Constructs a new S3 client for use with the integration test docker cluster. Creates a
   * new S3 client first time this is called, otherwise the existing S3 client is returned.
   *
   * @return an S3 client
   */
  def getS3Client(): AmazonS3 = {
    this.synchronized {
      if (s3Client == null) {
        s3Client = AmazonS3ClientBuilder.standard()
          .withEndpointConfiguration(new EndpointConfiguration("http://localhost:9000", "us-east-1"))
          .withCredentials(new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(getS3AccessKey(), getS3SecretKey())
          ))
          .withPathStyleAccessEnabled(true)
          .build()
      }

      s3Client
    }
  }
}
