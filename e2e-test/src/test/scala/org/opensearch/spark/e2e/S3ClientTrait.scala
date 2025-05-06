/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.spark.e2e

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

trait S3ClientTrait {
  var s3Client: AmazonS3 = null

  /**
   * Retrieves the S3 access key for the MinIO container.
   *
   * @return S3 access key for the MinIO container
   */
  def getS3AccessKey(): String

  /**
   * Retrieves the S3 secret key for the MinIO container.
   *
   * @return S3 secret key for the MinIO container
   */
  def getS3SecretKey(): String

  /**
   * Returns an AmazonS3 client. Constructs a new AmazonS3 client for use with the integration test docker cluster
   * MinIO container. Creates a new AmazonS3 client first time this is called, otherwise the existing S3 client is
   * returned.
   *
   * @return an AmazonS3 client for use with the integration test docker cluster
   */
  def getS3Client(): AmazonS3 = {
    this.synchronized {
      if (s3Client == null) {
        val credentials = new BasicAWSCredentials(getS3AccessKey(), getS3SecretKey())
        val endpointConfiguration = new EndpointConfiguration("http://localhost:9000", "us-east-1")

        s3Client = AmazonS3ClientBuilder.standard()
          .withCredentials(new AWSStaticCredentialsProvider(credentials))
          .withEndpointConfiguration(endpointConfiguration)
          .withPathStyleAccessEnabled(true)
          .build()

        ensureBucketExists("integ-test")
        ensureBucketExists("test-resources")
      }
      s3Client
    }
  }

  /**
   * Ensures that the specified bucket exists in the MinIO container.
   *
   * @param bucketName name of the bucket to ensure exists
   */
  def ensureBucketExists(bucketName: String): Unit = {
    if (!doesBucketExist(bucketName)) {
      s3Client.createBucket(bucketName)
    }
  }

  /**
   * Checks if the specified bucket exists in the MinIO container.
   *
   * @param bucketName name of the bucket to check
   * @return true if the bucket exists, false otherwise
   */
  def doesBucketExist(bucketName: String): Boolean = {
    s3Client.listBuckets().stream().anyMatch(bucket => bucket.getName == bucketName)
  }
}
