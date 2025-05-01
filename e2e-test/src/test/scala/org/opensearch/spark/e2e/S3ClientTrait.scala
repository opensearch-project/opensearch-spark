/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.spark.e2e

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.HeadBucketRequest
import org.apache.logging.log4j.{Logger, LogManager}

/**
 * Provides a method for obtaining an S3 client.
 */
trait S3ClientTrait {
  private val log: Logger = LogManager.getLogger(getClass)
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

   /**
   * Checks if an S3 bucket exists and is accessible
   *
   * @param bucketName name of the bucket to check
   * @return true if bucket exists and is accessible, false otherwise
   */
  def doesBucketExist(bucketName: String): Boolean = {
    try {
      getS3Client().headBucket(new HeadBucketRequest(bucketName))
      true
    } catch {
      case e: com.amazonaws.services.s3.model.AmazonS3Exception =>
        log.error(s"Error checking bucket '$bucketName': ${e.getMessage}")
        false
    }
  }

  /**
   * Ensures a bucket exists, creates it if it doesn't
   *
   * @param bucketName name of the bucket to check/create
   */
  def ensureBucketExists(bucketName: String): Unit = {
    if (!doesBucketExist(bucketName)) {
      log.info(s"Bucket '$bucketName' does not exist or is not accessible")
      try {
        getS3Client().createBucket(bucketName)
        log.info(s"Created bucket '$bucketName'")
      } catch {
        case e: Exception =>
          log.error(s"Failed to create bucket: ${e.getMessage}")
          throw e
      }
    }
  }
}
