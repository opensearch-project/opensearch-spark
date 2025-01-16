/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.spark.emrserverless;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.emrserverless.AWSEMRServerless;
import com.amazonaws.services.emrserverless.model.CancelJobRunRequest;
import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.CreateApplicationRequest;
import com.amazonaws.services.emrserverless.model.CreateApplicationResult;
import com.amazonaws.services.emrserverless.model.DeleteApplicationRequest;
import com.amazonaws.services.emrserverless.model.DeleteApplicationResult;
import com.amazonaws.services.emrserverless.model.GetApplicationRequest;
import com.amazonaws.services.emrserverless.model.GetApplicationResult;
import com.amazonaws.services.emrserverless.model.GetDashboardForJobRunRequest;
import com.amazonaws.services.emrserverless.model.GetDashboardForJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunRequest;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.ListApplicationsRequest;
import com.amazonaws.services.emrserverless.model.ListApplicationsResult;
import com.amazonaws.services.emrserverless.model.ListJobRunsRequest;
import com.amazonaws.services.emrserverless.model.ListJobRunsResult;
import com.amazonaws.services.emrserverless.model.ListTagsForResourceRequest;
import com.amazonaws.services.emrserverless.model.ListTagsForResourceResult;
import com.amazonaws.services.emrserverless.model.StartApplicationRequest;
import com.amazonaws.services.emrserverless.model.StartApplicationResult;
import com.amazonaws.services.emrserverless.model.StartJobRunRequest;
import com.amazonaws.services.emrserverless.model.StartJobRunResult;
import com.amazonaws.services.emrserverless.model.StopApplicationRequest;
import com.amazonaws.services.emrserverless.model.StopApplicationResult;
import com.amazonaws.services.emrserverless.model.TagResourceRequest;
import com.amazonaws.services.emrserverless.model.TagResourceResult;
import com.amazonaws.services.emrserverless.model.UntagResourceRequest;
import com.amazonaws.services.emrserverless.model.UntagResourceResult;
import com.amazonaws.services.emrserverless.model.UpdateApplicationRequest;
import com.amazonaws.services.emrserverless.model.UpdateApplicationResult;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class DockerEMRServerlessClient extends AmazonWebServiceClient implements AWSEMRServerless {
  private static final AtomicInteger JOB_ID = new AtomicInteger(1);

  public DockerEMRServerlessClient(ClientConfiguration clientConfiguration) {
    super(clientConfiguration);
    setEndpointPrefix("emr");
  }

  @Override
  public CancelJobRunResult cancelJobRun(final CancelJobRunRequest cancelJobRunRequest) {
    return null;
  }

  @Override
  public CreateApplicationResult createApplication(final CreateApplicationRequest createApplicationRequest) {
    return null;
  }

  @Override
  public DeleteApplicationResult deleteApplication(final DeleteApplicationRequest deleteApplicationRequest) {
    return null;
  }

  @Override
  public GetApplicationResult getApplication(final GetApplicationRequest getApplicationRequest) {
    return null;
  }

  @Override
  public GetDashboardForJobRunResult getDashboardForJobRun(
      final GetDashboardForJobRunRequest getDashboardForJobRunRequest) {
    return null;
  }

  @Override
  public GetJobRunResult getJobRun(final GetJobRunRequest getJobRunRequest) {
    return null;
  }

  @Override
  public ListApplicationsResult listApplications(final ListApplicationsRequest listApplicationsRequest) {
    return null;
  }

  @Override
  public ListJobRunsResult listJobRuns(final ListJobRunsRequest listJobRunsRequest) {
    return null;
  }

  @Override
  public ListTagsForResourceResult listTagsForResource(
      final ListTagsForResourceRequest listTagsForResourceRequest) {
    return null;
  }

  @Override
  public StartApplicationResult startApplication(final StartApplicationRequest startApplicationRequest) {
    return null;
  }

  @Override
  public StartJobRunResult startJobRun(final StartJobRunRequest startJobRunRequest) {
    String entryPoint = startJobRunRequest.getJobDriver().getSparkSubmit().getEntryPoint();
    List<String> entryPointArguments = startJobRunRequest.getJobDriver().getSparkSubmit().getEntryPointArguments();
    String sparkSubmitParameters = startJobRunRequest.getJobDriver().getSparkSubmit().getSparkSubmitParameters();

    final int jobId = JOB_ID.getAndIncrement();

    List<String> runContainerCmd = new ArrayList<>();
    runContainerCmd.add("run");
    runContainerCmd.add("-d");
    runContainerCmd.add("--rm");
    runContainerCmd.add("--env");
    runContainerCmd.add("SERVERLESS_EMR_JOB_ID=" + jobId);
    runContainerCmd.add("--network");
    runContainerCmd.add("integ-test_opensearch-net");
    runContainerCmd.add("integ-test-spark-submit:latest");
    runContainerCmd.add("/opt/bitnami/spark/bin/spark-submit");
    runContainerCmd.add("--deploy-mode");
    runContainerCmd.add("client");
    runContainerCmd.add("--exclude-packages");
    runContainerCmd.add("org.opensearch:opensearch-spark-standalone_2.12,org.opensearch:opensearch-spark-sql-application_2.12,org.opensearch:opensearch-spark-ppl_2.12");
    runContainerCmd.add("--master");
    runContainerCmd.add("local[2]");

    runContainerCmd.addAll(Arrays.asList(sparkSubmitParameters.split(" ")));
    runContainerCmd.addAll(entryPointArguments);

    final List<String> cmd = runContainerCmd.stream().filter(s -> !s.isBlank()).collect(Collectors.toList());

    for (int i = 1; i < cmd.size(); i++) {
      if (cmd.get(i - 1).equals("--conf")) {
        if (cmd.get(i).startsWith("spark.datasource.flint.customAWSCredentialsProvider=") ||
            cmd.get(i).startsWith("spark.datasource.flint.") ||
            cmd.get(i).startsWith("spark.hadoop.hive.metastore.client.factory.class=")) {
          cmd.remove(i - 1);
          cmd.remove(i - 1);
          i -= 2;
        } else if (cmd.get(i).startsWith("spark.emr-serverless.driverEnv.JAVA_HOME=")) {
          cmd.set(i, "spark.emr-serverless.driverEnv.JAVA_HOME=/opt/bitnami/java");
        } else if (cmd.get(i).startsWith("spark.executorEnv.JAVA_HOME=")) {
          cmd.set(i, "spark.executorEnv.JAVA_HOME=/opt/bitnami/java");
        }
      }
    }

    cmd.add(cmd.size() - 1, "/app/spark-sql-application.jar");

    System.out.println(">>> " + String.join(" ", cmd));

    try {
      File dockerDir = new File("/tmp/docker");
      File cmdFile = File.createTempFile("docker_", ".cmd", dockerDir);
      FileOutputStream fos = new FileOutputStream(cmdFile);
      fos.write(String.join(" ", cmd).getBytes(StandardCharsets.UTF_8));
      fos.close();

      String cmdFilename = cmdFile.getName();
      String filenameBase = cmdFilename.substring(7, cmdFilename.length() - 4);
      File exitCodeFile = new File(dockerDir, "docker_" + filenameBase + ".exitCode");
      for (int i = 0; i < 600 && !exitCodeFile.exists(); i++) {
        Thread.sleep(100L);
      }

      if (exitCodeFile.exists()) {
        StartJobRunResult startJobResult = new StartJobRunResult();
        startJobResult.setApplicationId(startJobRunRequest.getApplicationId());
        startJobResult.setArn("arn:aws:emr-containers:foo:123456789012:/virtualclusters/0/jobruns/" + jobId);
        startJobResult.setJobRunId(Integer.toString(jobId));
        return startJobResult;
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public StopApplicationResult stopApplication(final StopApplicationRequest stopApplicationRequest) {
    return null;
  }

  @Override
  public TagResourceResult tagResource(final TagResourceRequest tagResourceRequest) {
    return null;
  }

  @Override
  public UntagResourceResult untagResource(final UntagResourceRequest untagResourceRequest) {
    return null;
  }

  @Override
  public UpdateApplicationResult updateApplication(final UpdateApplicationRequest updateApplicationRequest) {
    return null;
  }

  @Override
  public ResponseMetadata getCachedResponseMetadata(final AmazonWebServiceRequest amazonWebServiceRequest) {
    return null;
  }
}
