/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.aws

import java.io.File
import java.time.LocalDateTime

import scala.concurrent.duration.DurationInt

import com.amazonaws.services.emrserverless.{AWSEMRServerless, AWSEMRServerlessClientBuilder}
import com.amazonaws.services.emrserverless.model.{GetJobRunRequest, JobDriver, SparkSubmit, StartJobRunRequest}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
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
  lazy val testServerlessHost: String = System.getenv("AWS_OPENSEARCH_SERVERLESS_HOST")
  lazy val testPort: Int = -1
  lazy val testRegion: String = System.getenv("AWS_REGION")
  lazy val testScheme: String = "https"
  lazy val testAuth: String = "sigv4"

  lazy val testAppId: String = System.getenv("AWS_EMRS_APPID")
  lazy val testExecutionRole: String = System.getenv("AWS_EMRS_EXECUTION_ROLE")
  lazy val testS3CodeBucket: String = System.getenv("AWS_S3_CODE_BUCKET")
  lazy val testS3CodePrefix: String = System.getenv("AWS_S3_CODE_PREFIX")
  lazy val testResultIndex: String = System.getenv("AWS_OPENSEARCH_RESULT_INDEX")

  "EMR Serverless job with AOS" should "run successfully" in {
    val s3Client = AmazonS3ClientBuilder.standard().withRegion(testRegion).build()
    val emrServerless = AWSEMRServerlessClientBuilder.standard().withRegion(testRegion).build()

    uploadJarsToS3(s3Client)

    val jobRunRequest = startJobRun("SELECT 1", testHost, "es")

    val jobRunResponse = emrServerless.startJobRun(jobRunRequest)

    verifyJobSucceed(emrServerless, jobRunResponse.getJobRunId)
  }

  "EMR Serverless job with AOSS" should "run successfully" in {
    val s3Client = AmazonS3ClientBuilder.standard().withRegion(testRegion).build()
    val emrServerless = AWSEMRServerlessClientBuilder.standard().withRegion(testRegion).build()

    uploadJarsToS3(s3Client)

    val jobRunRequest = startJobRun(
      "SELECT 1",
      testServerlessHost,
      "aoss",
      conf("spark.datasource.flint.write.refresh_policy", "false")
    )

    val jobRunResponse = emrServerless.startJobRun(jobRunRequest)

    verifyJobSucceed(emrServerless, jobRunResponse.getJobRunId)
  }

  private def verifyJobSucceed(emrServerless: AWSEMRServerless, jobRunId: String): Unit = {
    val startTime = System.currentTimeMillis()
    val timeout = 5.minutes.toMillis
    var jobState = "STARTING"

    while (System.currentTimeMillis() - startTime < timeout
      && (jobState != "FAILED" && jobState != "SUCCESS")) {
      Thread.sleep(30000)
      val request = new GetJobRunRequest()
        .withApplicationId(testAppId)
        .withJobRunId(jobRunId)
      jobState = emrServerless.getJobRun(request).getJobRun.getState
      logInfo(s"Current job state: $jobState at ${System.currentTimeMillis()}")
    }
    jobState shouldBe "SUCCESS"
  }

  private def startJobRun(query: String, host: String, authServiceName: String, additionalParams: String*) = {
    new StartJobRunRequest()
      .withApplicationId(testAppId)
      .withExecutionRoleArn(testExecutionRole)
      .withName(s"integration-${authServiceName}-${LocalDateTime.now()}")
      .withJobDriver(new JobDriver()
        .withSparkSubmit(new SparkSubmit()
          .withEntryPoint(s"s3://$testS3CodeBucket/$testS3CodePrefix/sql-job.jar")
          .withEntryPointArguments(testResultIndex)
          .withSparkSubmitParameters(
            join(
              clazz("org.apache.spark.sql.FlintJob"),
              jars(s"s3://$testS3CodeBucket/$testS3CodePrefix/extension.jar", s"s3://$testS3CodeBucket/$testS3CodePrefix/ppl.jar"),
              conf("spark.datasource.flint.host", host),
              conf("spark.datasource.flint.port", s"$testPort"),
              conf("spark.datasource.flint.scheme", testScheme),
              conf("spark.datasource.flint.auth", testAuth),
              conf("spark.datasource.flint.auth.servicename", authServiceName),
              conf("spark.sql.catalog.glue", "org.opensearch.sql.FlintDelegatingSessionCatalog"),
              conf("spark.flint.datasource.name", "glue"),
              conf("spark.flint.job.query", quote(query)),
              conf("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"),
              join(additionalParams: _*)
            )
          )
        )
      )
  }

  private def join(params: String*): String = params.mkString(" ")

  private def clazz(clazz: String): String = s"--class $clazz"

  private def jars(jars: String*): String = s"--jars ${jars.mkString(",")}"

  private def quote(str: String): String = "\"" + str + "\""

  private def conf(name: String, value: String): String = s"--conf $name=$value"

  private def uploadJarsToS3(s3Client: AmazonS3) = {
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
      new File(appJarPath))
    s3Client.putObject(
      testS3CodeBucket,
      s"$testS3CodePrefix/extension.jar",
      new File(extensionJarPath))
    s3Client.putObject(
      testS3CodeBucket,
      s"$testS3CodePrefix/ppl.jar",
      new File(pplJarPath))
  }
}
