# Developer Guide

## Package
If you want to package the single jar for, you can do so by running the following command:
```
sbt assembly
```

## Unit Test
To execute the unit tests, run the following command:
```
sbt test
```
To run a specific unit test in SBT, use the testOnly command with the full path of the test class:
```
sbt "; project pplSparkIntegration; test:testOnly org.opensearch.flint.spark.ppl.PPLLogicalPlanTrendlineCommandTranslatorTestSuite"
```


## Integration Test
The integration test is defined in the `integration` directory of the project. The integration tests will automatically trigger unit tests and will only run if all unit tests pass. If you want to run the integration test for the project, you can do so by running the following command:
```
sbt integtest/integration
```
If you get integration test failures with error message "Previous attempts to find a Docker environment failed" in macOS, fix the issue by following the checklist:
1. Check you've installed Docker in your dev host. If not, install Docker first.
2. Check if the file /var/run/docker.sock exists. If not, go to `3`.
3. Run `sudo ln -s $HOME/.docker/desktop/docker.sock /var/run/docker.sock` or `sudo ln -s $HOME/.docker/run/docker.sock /var/run/docker.sock`
4. If you use Docker Desktop, as an alternative of `3`, check mark the "Allow the default Docker socket to be used (requires password)" in advanced settings of Docker Desktop.

Running only a selected set of integration test suites is possible with the following command:
```
sbt "; project integtest; it:testOnly org.opensearch.flint.spark.ppl.FlintSparkPPLTrendlineITSuite"
```
This command runs only the specified test suite within the integtest submodule.


### AWS Integration Test
The `aws-integration` folder contains tests for cloud server providers. For instance, test against AWS OpenSearch domain, configure the following settings. The client will use the default credential provider to access the AWS OpenSearch domain.
```
export AWS_OPENSEARCH_HOST=search-xxx.us-west-2.on.aws
export AWS_OPENSEARCH_SERVERLESS_HOST=xxx.us-west-2.aoss.amazonaws.com
export AWS_REGION=us-west-2
export AWS_EMRS_APPID=xxx
export AWS_EMRS_EXECUTION_ROLE=xxx
export AWS_S3_CODE_BUCKET=xxx
export AWS_S3_CODE_PREFIX=xxx
export AWS_OPENSEARCH_RESULT_INDEX=query_execution_result_glue
```
And run the following command:
```
sbt integtest/awsIntegration

[info] AWSOpenSearchAccessTestSuite:
[info] - should Create Pit on AWS OpenSearch
[info] Run completed in 3 seconds, 116 milliseconds.
[info] Total number of tests run: 1
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 1, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
```

## Scala Formatting Guidelines

For Scala code, flint use [spark scalastyle](https://github.com/apache/spark/blob/master/scalastyle-config.xml). Before submitting the PR, 
make sure to use "scalafmtAll" to format the code. read more in [scalafmt sbt](https://scalameta.org/scalafmt/docs/installation.html#sbt)
```
sbt scalafmtAll
```
The code style is automatically checked, but users can also manually check it.
```
sbt scalastyle
```
For IntelliJ user, read more in [scalafmt IntelliJ](https://scalameta.org/scalafmt/docs/installation.html#intellij) to integrate 
scalafmt with IntelliJ
