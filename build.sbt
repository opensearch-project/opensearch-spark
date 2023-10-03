/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
import Dependencies._

lazy val scala212 = "2.12.14"
lazy val sparkVersion = "3.3.2"
lazy val opensearchVersion = "2.6.0"

ThisBuild / organization := "org.opensearch"

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := scala212

ThisBuild / scalafmtConfig := baseDirectory.value / "dev/.scalafmt.conf"

/**
 * ScalaStyle configurations
 */
ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

/**
 * Tests cannot be run in parallel since multiple Spark contexts cannot run in the same JVM
 */
ThisBuild / Test / parallelExecution := false

// Run as part of compile task.
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

// Run as part of test task.
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val commonSettings = Seq(
  javacOptions ++= Seq("-source", "11"),
  Compile / compile / javacOptions ++= Seq("-target", "11"),
  // Scalastyle
  scalastyleConfig := (ThisBuild / scalastyleConfig).value,
  compileScalastyle := (Compile / scalastyle).toTask("").value,
  Compile / compile := ((Compile / compile) dependsOn compileScalastyle).value,
  testScalastyle := (Test / scalastyle).toTask("").value,
  Test / test := ((Test / test) dependsOn testScalastyle).value)

lazy val root = (project in file("."))
  .aggregate(flintCore, flintSparkIntegration, pplSparkIntegration, sparkSqlApplication)
  .disablePlugins(AssemblyPlugin)
  .settings(name := "flint", publish / skip := true)

lazy val flintCore = (project in file("flint-core"))
  .disablePlugins(AssemblyPlugin)
  .settings(
    commonSettings,
    name := "flint-core",
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "org.opensearch.client" % "opensearch-rest-client" % opensearchVersion,
      "org.opensearch.client" % "opensearch-rest-high-level-client" % opensearchVersion
        exclude ("org.apache.logging.log4j", "log4j-api"),
      "com.amazonaws" % "aws-java-sdk" % "1.12.397" % "provided"
        exclude ("com.fasterxml.jackson.core", "jackson-databind")),
    publish / skip := true)

lazy val pplSparkIntegration = (project in file("ppl-spark-integration"))
  .enablePlugins(AssemblyPlugin, Antlr4Plugin)
  .settings(
    commonSettings,
    name := "ppl-spark-integration",
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "org.scalactic" %% "scalactic" % "3.2.15" % "test",
      "org.scalatest" %% "scalatest" % "3.2.15" % "test",
      "org.scalatest" %% "scalatest-flatspec" % "3.2.15" % "test",
      "org.scalatestplus" %% "mockito-4-6" % "3.2.15.0" % "test",
      "com.stephenn" %% "scalatest-json-jsonassert" % "0.2.5" % "test",
      "com.github.sbt" % "junit-interface" % "0.13.3" % "test"),
    libraryDependencies ++= deps(sparkVersion),
    // ANTLR settings
    Antlr4 / antlr4Version := "4.8",
    Antlr4 / antlr4PackageName := Some("org.opensearch.flint.spark.ppl"),
    Antlr4 / antlr4GenListener := true,
    Antlr4 / antlr4GenVisitor := true,
    // Assembly settings
    assemblyPackageScala / assembleArtifact := false,
    assembly / assemblyOption ~= {
      _.withIncludeScala(false)
    },
    assembly / assemblyMergeStrategy := {
      case PathList(ps @ _*) if ps.last endsWith ("module-info.class") =>
        MergeStrategy.discard
      case PathList("module-info.class") => MergeStrategy.discard
      case PathList("META-INF", "versions", xs @ _, "module-info.class") =>
        MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / test := (Test / test).value)

lazy val flintSparkIntegration = (project in file("flint-spark-integration"))
  .dependsOn(flintCore)
  .enablePlugins(AssemblyPlugin, Antlr4Plugin)
  .settings(
    commonSettings,
    name := "flint-spark-integration",
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk" % "1.12.397" % "provided"
        exclude ("com.fasterxml.jackson.core", "jackson-databind"),
      "org.scalactic" %% "scalactic" % "3.2.15" % "test",
      "org.scalatest" %% "scalatest" % "3.2.15" % "test",
      "org.scalatest" %% "scalatest-flatspec" % "3.2.15" % "test",
      "org.scalatestplus" %% "mockito-4-6" % "3.2.15.0" % "test",
      "com.stephenn" %% "scalatest-json-jsonassert" % "0.2.5" % "test",
      "com.github.sbt" % "junit-interface" % "0.13.3" % "test"),
    libraryDependencies ++= deps(sparkVersion),
    // ANTLR settings
    Antlr4 / antlr4Version := "4.8",
    Antlr4 / antlr4PackageName := Some("org.opensearch.flint.spark.sql"),
    Antlr4 / antlr4GenListener := true,
    Antlr4 / antlr4GenVisitor := true,
    // Assembly settings
    assemblyPackageScala / assembleArtifact := false,
    assembly / assemblyOption ~= {
      _.withIncludeScala(false)
    },
    assembly / assemblyMergeStrategy := {
      case PathList(ps @ _*) if ps.last endsWith ("module-info.class") =>
        MergeStrategy.discard
      case PathList("module-info.class") => MergeStrategy.discard
      case PathList("META-INF", "versions", xs @ _, "module-info.class") =>
        MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / test := (Test / test).value)

// Test assembly package with integration test.
lazy val integtest = (project in file("integ-test"))
  .dependsOn(flintSparkIntegration % "test->test", pplSparkIntegration % "test->test" )
  .settings(
    commonSettings,
    name := "integ-test",
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk" % "1.12.397" % "provided"
        exclude ("com.fasterxml.jackson.core", "jackson-databind"),
      "org.scalactic" %% "scalactic" % "3.2.15",
      "org.scalatest" %% "scalatest" % "3.2.15" % "test",
      "com.stephenn" %% "scalatest-json-jsonassert" % "0.2.5" % "test",
      "org.testcontainers" % "testcontainers" % "1.18.0" % "test",
      // add opensearch-java client to get node stats
      "org.opensearch.client" % "opensearch-java" % "2.6.0" % "test"
        exclude ("com.fasterxml.jackson.core", "jackson-databind")),
    libraryDependencies ++= deps(sparkVersion),
    Test / fullClasspath ++= Seq((flintSparkIntegration / assembly).value, (pplSparkIntegration / assembly).value))

lazy val standaloneCosmetic = project
  .settings(
    name := "opensearch-spark-standalone",
    commonSettings,
    releaseSettings,
    exportJars := true,
    Compile / packageBin := (flintSparkIntegration / assembly).value)

lazy val sparkSqlApplication = (project in file("spark-sql-application"))
  .settings(
    commonSettings,
    name := "sql-job",
    scalaVersion := scala212,
    libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % "3.2.15" % "test"),
    libraryDependencies ++= deps(sparkVersion))

lazy val sparkSqlApplicationCosmetic = project
  .settings(
    name := "opensearch-spark-sql-application",
    commonSettings,
    releaseSettings,
    exportJars := true,
    Compile / packageBin := (sparkSqlApplication / assembly).value)

lazy val sparkPPLCosmetic = project
  .settings(
    name := "opensearch-spark-ppl",
    commonSettings,
    releaseSettings,
    exportJars := true,
    Compile / packageBin := (pplSparkIntegration / assembly).value)

lazy val releaseSettings = Seq(
  publishMavenStyle := true,
  publishArtifact := true,
  Test / publishArtifact := false,
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
  pomExtra :=
    <url>https://opensearch.org/</url>
      <scm>
        <url>git@github.com:opensearch-project/opensearch-spark.git</url>
        <connection>scm:git:git@github.com:opensearch-project/opensearch-spark.git</connection>
      </scm>)
