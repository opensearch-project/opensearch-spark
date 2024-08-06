/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.aws

import java.time.LocalDateTime

import scala.concurrent.duration.DurationInt

import com.amazonaws.services.emrserverless.AWSEMRServerlessClientBuilder
import com.amazonaws.services.emrserverless.model.{GetJobRunRequest, JobDriver, SparkSubmit, StartJobRunRequest}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.apache.spark.internal.Logging

class AWSEmrServerlessAccessTestSuite
    extends AnyFlatSpec
    with BeforeAndAfter
    with Matchers
    with Logging {

  lazy val testHost: String = System.getenv("AWS_OPENSEARCH_HOST")
  lazy val testPort: Int = -1
  lazy val testRegion: String = System.getenv("AWS_REGION")
  lazy val testScheme: String = "https"
  lazy val testAuth: String = "sigv4"

  lazy val testAppId: String = System.getenv("AWS_EMRS_APPID")
  lazy val testExecutionRole: String = System.getenv("AWS_EMRS_EXECUTION_ROLE")
  lazy val testS3CodeBucket: String = System.getenv("AWS_S3_CODE_BUCKET")
  lazy val testS3CodePrefix: String = System.getenv("AWS_S3_CODE_PREFIX")
  lazy val testResultIndex: String = System.getenv("AWS_OPENSEARCH_RESULT_INDEX")

  "EMR Serverless job" should "run successfully" in {
    val s3Client = AmazonS3ClientBuilder.standard().withRegion(testRegion).build()
    val emrServerless = AWSEMRServerlessClientBuilder.standard().withRegion(testRegion).build()

    val appJarPath =
      sys.props.getOrElse("appJar", throw new IllegalArgumentException("appJar not set"))
    val extensionJarPath = sys.props.getOrElse(
      "extensionJar",
      throw new IllegalArgumentException("extensionJar not set"))
    val pplJarPath =
      sys.props.getOrElse("pplJar", throw new IllegalArgumentException("pplJar not set"))

    s3Client.putObject(
      testS3CodeBucket,
      s"$testS3CodePrefix/sql-job.jar",
      new java.io.File(appJarPath))
    s3Client.putObject(
      testS3CodeBucket,
      s"$testS3CodePrefix/extension.jar",
      new java.io.File(extensionJarPath))
    s3Client.putObject(
      testS3CodeBucket,
      s"$testS3CodePrefix/ppl.jar",
      new java.io.File(pplJarPath))

    val jobRunRequest = new StartJobRunRequest()
      .withApplicationId(testAppId)
      .withExecutionRoleArn(testExecutionRole)
      .withName(s"integration-${LocalDateTime.now()}")
      .withJobDriver(new JobDriver()
        .withSparkSubmit(new SparkSubmit()
          .withEntryPoint(s"s3://$testS3CodeBucket/$testS3CodePrefix/sql-job.jar")
          .withEntryPointArguments(testResultIndex)
          .withSparkSubmitParameters(s"--class org.apache.spark.sql.FlintJob --jars " +
            s"s3://$testS3CodeBucket/$testS3CodePrefix/extension.jar," +
            s"s3://$testS3CodeBucket/$testS3CodePrefix/ppl.jar " +
            s"--conf spark.datasource.flint.host=$testHost " +
            s"--conf spark.datasource.flint.port=-1  " +
            s"--conf spark.datasource.flint.scheme=$testScheme  " +
            s"--conf spark.datasource.flint.auth=$testAuth " +
            s"--conf spark.sql.catalog.glue=org.opensearch.sql.FlintDelegatingSessionCatalog  " +
            s"--conf spark.flint.datasource.name=glue " +
            s"""--conf spark.flint.job.query="SELECT 1" """ +
            s"--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")))

    val jobRunResponse = emrServerless.startJobRun(jobRunRequest)

    val startTime = System.currentTimeMillis()
    val timeout = 5.minutes.toMillis
    var jobState = "STARTING"

    while (System.currentTimeMillis() - startTime < timeout
      && (jobState != "FAILED" && jobState != "SUCCESS")) {
      Thread.sleep(30000)
      val request = new GetJobRunRequest()
        .withApplicationId(testAppId)
        .withJobRunId(jobRunResponse.getJobRunId)
      jobState = emrServerless.getJobRun(request).getJobRun.getState
      logInfo(s"Current job state: $jobState at ${System.currentTimeMillis()}")
    }

    jobState shouldBe "SUCCESS"
  }
}
