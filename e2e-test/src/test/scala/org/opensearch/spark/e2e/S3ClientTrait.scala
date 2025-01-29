/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.spark.e2e

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

trait S3ClientTrait {
  var s3Client : AmazonS3 = null

  def getS3AccessKey(): String

  def getS3SecretKey(): String

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
