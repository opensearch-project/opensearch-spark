/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
import Dependencies._

lazy val scala212 = "2.12.14"
lazy val sparkVersion = "3.3.2"
lazy val opensearchVersion = "2.6.0"
lazy val icebergVersion = "1.5.0"

val scalaMinorVersion = scala212.split("\\.").take(2).mkString(".")
val sparkMinorVersion = sparkVersion.split("\\.").take(2).mkString(".")

ThisBuild / organization := "org.opensearch"

ThisBuild / version := "0.3.0-SNAPSHOT"

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

// running `scalafmtAll` includes all subprojects under root
lazy val root = (project in file("."))
  .aggregate(flintCore, flintSparkIntegration, pplSparkIntegration, sparkSqlApplication, integtest)
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
      "dev.failsafe" % "failsafe" % "3.3.2",
      "com.amazonaws" % "aws-java-sdk" % "1.12.397" % "provided"
        exclude ("com.fasterxml.jackson.core", "jackson-databind"),
      "com.amazonaws" % "aws-java-sdk-cloudwatch" % "1.12.593"
        exclude("com.fasterxml.jackson.core", "jackson-databind"),
      "org.scalactic" %% "scalactic" % "3.2.15" % "test",
      "org.scalatest" %% "scalatest" % "3.2.15" % "test",
      "org.scalatest" %% "scalatest-flatspec" % "3.2.15" % "test",
      "org.scalatestplus" %% "mockito-4-6" % "3.2.15.0" % "test",
      "com.stephenn" %% "scalatest-json-jsonassert" % "0.2.5" % "test",
      "org.mockito" % "mockito-core" % "4.6.1" % "test",
      "org.mockito" % "mockito-inline" % "4.6.1" % "test",
      "org.mockito" % "mockito-junit-jupiter" % "3.12.4" % "test",
      "org.junit.jupiter" % "junit-jupiter-api" % "5.9.0" % "test",
      "org.junit.jupiter" % "junit-jupiter-engine" % "5.9.0" % "test",
      "com.google.truth" % "truth" % "1.1.5" % "test",
      "net.aichler" % "jupiter-interface" % "0.11.1" % Test
    ),
    libraryDependencies ++= deps(sparkVersion),
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
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp filter { file => file.data.getName.contains("LogsConnectorSpark")}
    },
    assembly / test := (Test / test).value)

// Test assembly package with integration test.
lazy val integtest = (project in file("integ-test"))
  .dependsOn(flintSparkIntegration % "test->test", pplSparkIntegration % "test->test", sparkSqlApplication % "test->test")
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
      "org.apache.iceberg" %% s"iceberg-spark-runtime-$sparkMinorVersion" % icebergVersion % "test",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0" % "test",
      // add opensearch-java client to get node stats
      "org.opensearch.client" % "opensearch-java" % "2.6.0" % "test"
        exclude ("com.fasterxml.jackson.core", "jackson-databind")),
    libraryDependencies ++= deps(sparkVersion),
    Test / fullClasspath ++= Seq((flintSparkIntegration / assembly).value, (pplSparkIntegration / assembly).value,
      (sparkSqlApplication / assembly).value
    ))

lazy val standaloneCosmetic = project
  .settings(
    name := "opensearch-spark-standalone",
    commonSettings,
    releaseSettings,
    exportJars := true,
    Compile / packageBin := (flintSparkIntegration / assembly).value)

lazy val sparkSqlApplication = (project in file("spark-sql-application"))
  // dependency will be provided at runtime, so it doesn't need to be included in the assembled JAR
  .dependsOn(flintSparkIntegration % "provided")
  .settings(
    commonSettings,
    name := "sql-job",
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.15" % "test"),
    libraryDependencies ++= deps(sparkVersion),
    libraryDependencies ++= Seq(
      "com.typesafe.play" %% "play-json" % "2.9.2",
      // handle AmazonS3Exception
      "com.amazonaws" % "aws-java-sdk-s3" % "1.12.568" % "provided"
        // the transitive jackson.core dependency conflicts with existing scala
        // error: Scala module 2.13.4 requires Jackson Databind version >= 2.13.0 and < 2.14.0 -
        // Found jackson-databind version 2.14.2
        exclude ("com.fasterxml.jackson.core", "jackson-databind"),
      "org.scalatest" %% "scalatest" % "3.2.15" % "test",
      "org.mockito" %% "mockito-scala" % "1.16.42" % "test",
      "org.scalatestplus" %% "mockito-4-6" % "3.2.15.0" % "test"),
    // Assembly settings
    // the sbt assembly plugin found multiple copies of the module-info.class file with
    // different contents in the jars  that it was merging flintCore dependencies.
    // This can happen if you have multiple dependencies that include the same library,
    // but with different versions.
    assemblyPackageScala / assembleArtifact := false,
    assembly / assemblyOption ~= {
      _.withIncludeScala(false)
    },
    assembly / assemblyMergeStrategy := {
      case PathList(ps@_*) if ps.last endsWith ("module-info.class") =>
        MergeStrategy.discard
      case PathList("module-info.class") => MergeStrategy.discard
      case PathList("META-INF", "versions", xs@_, "module-info.class") =>
        MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / test := (Test / test).value
  )

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
