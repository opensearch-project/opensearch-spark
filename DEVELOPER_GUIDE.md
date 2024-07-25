# Developer Guide

## Package
If you want to package the single jar for, you can do so by running the following command:
```
sbt assembly
```

## Integration Test
The integration test is defined in the integ-test directory of the project. If you want to run the integration test for the project, you 
can do so by running the following command:
```
sbt integtest/test
```
If you get integration test failures with error message "Previous attempts to find a Docker environment failed" in macOS, fix the issue by following the checklist:
1. Check you've installed Docker in your dev host. If not, install Docker first.
2. Check if the file /var/run/docker.sock exists. If not, go to `3`.
3. Run `sudo ln -s $HOME/.docker/desktop/docker.sock /var/run/docker.sock` or `sudo ln -s $HOME/.docker/run/docker.sock /var/run/docker.sock`
4. If you use Docker Desktop, as an alternative of `3`, check mark the "Allow the default Docker socket to be used (requires password)" in advanced settings of Docker Desktop.

### AWS Integration Test
The integration folder contains tests for cloud server providers. For instance, test against AWS OpenSearch domain, configure the following settings. The client will use the default credential provider to access the AWS OpenSearch domain.
```
export AWS_OPENSEARCH_HOST=search-xxx.aos.us-west-2.on.aws
export AWS_REGION=us-west-2
```
And run the 
```
sbt integtest/integration

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
