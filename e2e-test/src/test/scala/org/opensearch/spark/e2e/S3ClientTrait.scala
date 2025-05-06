/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.spark.e2e

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.slf4j.{Logger, LoggerFactory}

trait S3ClientTrait {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
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
        try {
          // First try with the configured credentials
          val credentials = new BasicAWSCredentials(getS3AccessKey(), getS3SecretKey())
          val endpointConfiguration = new EndpointConfiguration("http://localhost:9000", "us-east-1")

          s3Client = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withEndpointConfiguration(endpointConfiguration)
            .withPathStyleAccessEnabled(true)
            .build()
          // Test the connection by listing buckets
          s3Client.listBuckets()
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to connect with configured credentials: ${e.getMessage}")
            logger.info("Falling back to default MinIO credentials (minioadmin/minioadmin)")
            // Fall back to default MinIO credentials
            val defaultCredentials = new BasicAWSCredentials("minioadmin", "minioadmin")
            val endpointConfiguration = new EndpointConfiguration("http://localhost:9000", "us-east-1")
            s3Client = AmazonS3ClientBuilder.standard()
              .withCredentials(new AWSStaticCredentialsProvider(defaultCredentials))
              .withEndpointConfiguration(endpointConfiguration)
              .withPathStyleAccessEnabled(true)
              .build()
        }

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
