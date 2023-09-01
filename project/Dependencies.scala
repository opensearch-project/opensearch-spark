/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import sbt._

object Dependencies {
  /**
   * add spark related dependencies
   * @param sparkVersion
   * @return
   */
  def sparkDeps(sparkVersion: String): Seq[ModuleID] = {
    Seq(
      "org.json4s" %% "json4s-native" % "3.7.0-M5",
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources (),
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" withSources (),
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided" withSources (),
      "org.json4s" %% "json4s-native" % "3.7.0-M5" % "test",
      "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests"
    )
  }

  /**
   * add opensearch related dependencies
   * @param opensearchVersion
   * @param opensearchClientVersion
   * @return
   */
  def osDeps(opensearchVersion: String, opensearchClientVersion: String): Seq[ModuleID] = {
    Seq(
      "org.opensearch.client" % "opensearch-rest-client" % opensearchClientVersion,
      "org.opensearch.client" % "opensearch-rest-high-level-client" % opensearchClientVersion
        exclude("org.apache.logging.log4j", "log4j-api"),
    )
  }
}
