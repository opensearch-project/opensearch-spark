/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
import Dependencies.*
import java.io.File
import java.net.URL
import java.util.Locale
import java.util.zip.ZipFile
import scala.collection.JavaConverters.*
import scala.xml.XML

lazy val scala212 = "2.12.14"
lazy val sparkVersion = "3.5.1"
// Spark jackson version. Spark jackson-module-scala strictly check the jackson-databind version should compatible
// https://github.com/FasterXML/jackson-module-scala/blob/2.18/src/main/scala/com/fasterxml/jackson/module/scala/JacksonModule.scala#L59
lazy val jacksonVersion = "2.15.2"

// The transitive opensearch jackson-databind dependency version should align with Spark jackson databind dependency version.
// Issue: https://github.com/opensearch-project/opensearch-spark/issues/442
lazy val opensearchVersion = "2.6.0"
lazy val opensearchMavenVersion = "2.6.0.0"
lazy val icebergVersion = "1.5.0"

val scalaMinorVersion = scala212.split("\\.").take(2).mkString(".")
val sparkMinorVersion = sparkVersion.split("\\.").take(2).mkString(".")

ThisBuild / organization := "org.opensearch"

ThisBuild / version := "0.8.0-commit-metadata-poc-SNAPSHOT"

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

/**
 * Set the parallelism of forked tests to 4 to accelerate integration test
 */
concurrentRestrictions in Global := Seq(Tags.limit(Tags.ForkedTestGroup, 4))

// Run as part of compile task.
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

// Run as part of test task.
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

// Explanation:
// - ThisBuild / assemblyShadeRules sets the shading rules for the entire build
// - ShadeRule.rename(...) creates a rule to rename multiple package patterns
// - "shaded.@0" means prepend "shaded." to the original package name
// - .inAll applies the rule to all dependencies, not just direct dependencies
val packagesToShade = Seq(
  "com.amazonaws.cloudwatch.**",
  "com.google.**",
  "com.sun.jna.**",
  "com.thoughtworks.paranamer.**",
  "javax.annotation.**",
  "org.apache.commons.codec.**",
  "org.apache.commons.logging.**",
  "org.apache.hc.**",
  "org.apache.http.**",
  "org.glassfish.json.**",
  "org.joda.time.**",
  "org.reactivestreams.**",
  "org.yaml.**"
)

ThisBuild / assemblyShadeRules := Seq(
  ShadeRule.rename(
    packagesToShade.map(_ -> "shaded.flint.@0"): _*
  ).inAll
)

lazy val commonSettings = Seq(
  javacOptions ++= Seq("-source", "11"),
  Compile / compile / javacOptions ++= Seq("-target", "11"),
  // Scalastyle
  scalastyleConfig := (ThisBuild / scalastyleConfig).value,
  compileScalastyle := (Compile / scalastyle).toTask("").value,
  Compile / compile := ((Compile / compile) dependsOn compileScalastyle).value,
  testScalastyle := (Test / scalastyle).toTask("").value,
  // Enable HTML report and output to separate folder per package
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", s"target/test-reports/${name.value}"),
  Test / test := ((Test / test) dependsOn testScalastyle).value,
  // Needed for HTML report
  libraryDependencies += "com.vladsch.flexmark" % "flexmark-all" % "0.64.8" % "test",
  dependencyOverrides ++= Seq(
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
  ))

// running `scalafmtAll` includes all subprojects under root
lazy val root = (project in file("."))
  .aggregate(flintCommons, flintCore, flintSparkIntegration, pplSparkIntegration, sparkSqlApplication, integtest)
  .disablePlugins(AssemblyPlugin)
  .settings(name := "flint", publish / skip := true)

lazy val flintCore = (project in file("flint-core"))
  .disablePlugins(AssemblyPlugin)
  .dependsOn(flintCommons)
  .settings(
    commonSettings,
    name := "flint-core",
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "org.opensearch.client" % "opensearch-rest-client" % opensearchVersion,
      "org.opensearch.client" % "opensearch-rest-high-level-client" % opensearchVersion
        exclude ("org.apache.logging.log4j", "log4j-api"),
      "org.opensearch.client" % "opensearch-java" % opensearchVersion
        // error: Scala module 2.13.4 requires Jackson Databind version >= 2.13.0 and < 2.14.0 -
        // Found jackson-databind version 2.14.
        exclude ("com.fasterxml.jackson.core", "jackson-databind")
        exclude ("com.fasterxml.jackson.core", "jackson-core")
        exclude ("org.apache.httpcomponents.client5", "httpclient5"),
      "org.opensearch" % "opensearch-job-scheduler-spi" % opensearchMavenVersion,
      "dev.failsafe" % "failsafe" % "3.3.2",
      "com.google.guava" % "guava" % "33.3.1-jre",
      "com.amazonaws" % "aws-java-sdk" % "1.12.397" % "provided"
        exclude ("com.fasterxml.jackson.core", "jackson-databind"),
      "com.amazonaws" % "aws-java-sdk-cloudwatch" % "1.12.593"
        exclude("com.fasterxml.jackson.core", "jackson-databind"),
      "software.amazon.awssdk" % "auth-crt" % "2.28.10",
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "org.projectlombok" % "lombok" % "1.18.30" % "provided",
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
      "com.typesafe.play" %% "play-json" % "2.9.2" % "test",
      "com.google.truth" % "truth" % "1.1.5" % "test",
      "net.aichler" % "jupiter-interface" % "0.11.1" % Test
    ),
    libraryDependencies ++= deps(sparkVersion),
    publish / skip := true)


lazy val flintCommons = (project in file("flint-commons"))
  .settings(
    commonSettings,
    name := "flint-commons",
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "org.scalactic" %% "scalactic" % "3.2.15" % "test",
      "org.scalatest" %% "scalatest" % "3.2.15" % "test",
      "org.scalatest" %% "scalatest-flatspec" % "3.2.15" % "test",
      "org.scalatestplus" %% "mockito-4-6" % "3.2.15.0" % "test",
      "org.projectlombok" % "lombok" % "1.18.30" % "provided",
    ),
    libraryDependencies ++= deps(sparkVersion),
    publish / skip := true,
    assembly / test := (Test / test).value,
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
  )
  .enablePlugins(AssemblyPlugin)


// Add the Maven repository for grammar dependencies at project root level
resolvers += "AWS OSS Sonatype Snapshots" at "https://aws.oss.sonatype.org/content/repositories/snapshots"



// Define task keys for grammar extraction
lazy val downloadPplGrammar = taskKey[Seq[File]]("Download and extract PPL grammar files")
lazy val downloadFlintGrammar = taskKey[Seq[File]]("Download and extract Flint grammar files")

// Helper to find latest snapshot version and construct proper URL
def findLatestSnapshotArtifactInfo(artifactId: String, version: String): (String, String) = {
  val metadataUrl = s"https://aws.oss.sonatype.org/content/repositories/snapshots/org/opensearch/$artifactId/$version/maven-metadata.xml"

  try {
    val metadata = XML.load(new URL(metadataUrl))
    val timestamp = (metadata \\ "timestamp").text
    val buildNumber = (metadata \\ "buildNumber").text

    if (timestamp.isEmpty || buildNumber.isEmpty) {
      throw new RuntimeException(s"Could not find timestamp or buildNumber in maven-metadata.xml for $artifactId $version")
    }

    // Maven snapshots use baseVersion-timestamp-buildNumber format (without -SNAPSHOT)
    // Split the version into base and qualifier parts
    val baseVersion = if (version.endsWith("-SNAPSHOT")) version.substring(0, version.length - 9) else version
    val snapshotVersion = s"$baseVersion-$timestamp-$buildNumber"

    (baseVersion, snapshotVersion)
  } catch {
    case e: Exception =>
      throw new RuntimeException(s"Error fetching metadata from $metadataUrl", e)
  }
}

// Helper to download a file from URL
def downloadFile(url: String, targetFile: File): File = {
  val connection = new URL(url).openConnection()
  connection.setConnectTimeout(10000)
  connection.setReadTimeout(30000)

  IO.transfer(connection.getInputStream, targetFile)
  targetFile
}

// Enhanced helper to extract grammar files with better debugging and discovery
def extractGrammarFiles(zipFile: File, targetDir: File, grammarFiles: Seq[String], log: Logger): Seq[File] = {
  IO.createDirectory(targetDir)

  // First, let's inspect the content of the zip
  val zipEntries = new ZipFile(zipFile)
  val entries = zipEntries.entries.asScala.toList

  log.info(s"ZIP contains ${entries.size} entries")

  // Log file structure for debugging
  entries.sortBy(_.getName).foreach { entry =>
    log.info(s"ZIP entry: ${entry.getName}")
  }

  // Try to find the grammar files in a more flexible way
  val extractedFiles = grammarFiles.map { grammarFile =>
    // Find all matching entries, case-insensitive
    val matchingEntries = entries.filter { entry =>
      entry.getName.toLowerCase(Locale.ROOT).endsWith(grammarFile.toLowerCase(Locale.ROOT))
    }

    if (matchingEntries.isEmpty) {
      log.error(s"Could not find $grammarFile in ZIP file. No matching entries.")
      throw new RuntimeException(s"Could not find $grammarFile in zip file")
    }

    // Get the best match (usually there's just one)
    val bestMatch = matchingEntries.head
    log.info(s"Found matching entry for $grammarFile: ${bestMatch.getName}")

    // Extract this specific file
    val targetFile = targetDir / grammarFile
    IO.transfer(zipEntries.getInputStream(bestMatch), targetFile)
    log.info(s"Extracted $grammarFile to $targetFile")

    targetFile
  }

  zipEntries.close()
  extractedFiles
}

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
      "com.github.sbt" % "junit-interface" % "0.13.3" % "test",
      "org.projectlombok" % "lombok" % "1.18.30",
      "com.github.seancfoley" % "ipaddress" % "5.5.1"
    ),
    libraryDependencies ++= deps(sparkVersion),

    // Define the grammar extraction task
    downloadPplGrammar := {
      val log = streams.value.log
      val baseDir = (Antlr4 / sourceDirectory).value
      val grammarFiles = Seq("OpenSearchPPLLexer.g4", "OpenSearchPPLParser.g4")
      val artifactId = "language-grammar"
      val version = "0.1.0-SNAPSHOT"

      // Create temp directory for downloads
      val tempDir = IO.createTemporaryDirectory

      // Get latest version
      log.info(s"Finding latest snapshot version for $artifactId $version")
      val (baseVersion, snapshotVersion) = findLatestSnapshotArtifactInfo(artifactId, version)
      log.info(s"Found latest snapshot version: $snapshotVersion")

      // Download zip file
      val zipUrl = s"https://aws.oss.sonatype.org/content/repositories/snapshots/org/opensearch/$artifactId/$version/$artifactId-$snapshotVersion.zip"
      val zipFile = tempDir / s"$artifactId-$snapshotVersion.zip"
      log.info(s"Downloading grammar from $zipUrl")
      downloadFile(zipUrl, zipFile)
      log.info(s"Downloaded grammar to $zipFile")

      // Extract grammar files with enhanced logging and discovery
      val extractedFiles = extractGrammarFiles(zipFile, baseDir, grammarFiles, log)
      log.info(s"Extracted ${extractedFiles.size} grammar files: ${extractedFiles.map(_.getName).mkString(", ")}")

      // Clean up
      IO.delete(tempDir)

      extractedFiles
    },

    // Make ANTLR generation depend on grammar download
    Antlr4 / antlr4Generate := {
      val _ = downloadPplGrammar.value
      (Antlr4 / antlr4Generate).value
    },

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
    assembly / test := (Test / test).value
  )

lazy val flintSparkIntegration = (project in file("flint-spark-integration"))
  .dependsOn(flintCore, flintCommons)
  .enablePlugins(AssemblyPlugin, Antlr4Plugin)
  .settings(
    commonSettings,
    name := "flint-spark-integration",
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk" % "1.12.397" % "provided"
        exclude ("com.fasterxml.jackson.core", "jackson-databind"),
      "org.locationtech.jts" % "jts-core" % "1.20.0",
      "org.scalactic" %% "scalactic" % "3.2.15" % "test",
      "org.scalatest" %% "scalatest" % "3.2.15" % "test",
      "org.scalatest" %% "scalatest-flatspec" % "3.2.15" % "test",
      "org.scalatestplus" %% "mockito-4-6" % "3.2.15.0" % "test",
      "org.mockito" % "mockito-inline" % "4.6.0" % "test",
      "com.stephenn" %% "scalatest-json-jsonassert" % "0.2.5" % "test",
      "com.github.seancfoley" % "ipaddress" % "5.5.1",
      "com.github.sbt" % "junit-interface" % "0.13.3" % "test"
    ),
    libraryDependencies ++= deps(sparkVersion),

    // Define the grammar extraction task
    downloadFlintGrammar := {
      val log = streams.value.log
      val baseDir = (Antlr4 / sourceDirectory).value
      val grammarFiles = Seq("FlintSparkSqlExtensions.g4", "SparkSqlBase.g4")
      val artifactId = "language-grammar"
      val version = "0.1.0-SNAPSHOT"

      // Create temp directory for downloads
      val tempDir = IO.createTemporaryDirectory

      // Get latest version
      log.info(s"Finding latest snapshot version for $artifactId $version")
      val (baseVersion, snapshotVersion) = findLatestSnapshotArtifactInfo(artifactId, version)
      log.info(s"Found latest snapshot version: $snapshotVersion")

      // Download zip file
      val zipUrl = s"https://aws.oss.sonatype.org/content/repositories/snapshots/org/opensearch/$artifactId/$version/$artifactId-$snapshotVersion.zip"
      val zipFile = tempDir / s"$artifactId-$snapshotVersion.zip"
      log.info(s"Downloading grammar from $zipUrl")
      downloadFile(zipUrl, zipFile)
      log.info(s"Downloaded grammar to $zipFile")

      // Extract grammar files with enhanced logging and discovery
      val extractedFiles = extractGrammarFiles(zipFile, baseDir, grammarFiles, log)
      log.info(s"Extracted ${extractedFiles.size} grammar files: ${extractedFiles.map(_.getName).mkString(", ")}")

      // Clean up
      IO.delete(tempDir)

      extractedFiles
    },

    // Make ANTLR generation depend on grammar download
    Antlr4 / antlr4Generate := {
      val _ = downloadFlintGrammar.value
      (Antlr4 / antlr4Generate).value
    },

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
    assembly / test := (Test / test).value
  )

lazy val IntegrationTest = config("it") extend Test
lazy val AwsIntegrationTest = config("aws-it") extend Test

// Test assembly package with integration test.
lazy val integtest = (project in file("integ-test"))
  .dependsOn(flintCommons % "test->test", flintSparkIntegration % "test->test", pplSparkIntegration % "test->test", sparkSqlApplication % "test->test")
  .configs(IntegrationTest, AwsIntegrationTest)
  .settings(
    commonSettings,
    name := "integ-test",
    scalaVersion := scala212,
    javaOptions ++= Seq(
      s"-DappJar=${(sparkSqlApplication / assembly).value.getAbsolutePath}",
      s"-DextensionJar=${(flintSparkIntegration / assembly).value.getAbsolutePath}",
      s"-DpplJar=${(pplSparkIntegration / assembly).value.getAbsolutePath}",
    ),
    inConfig(IntegrationTest)(Defaults.testSettings ++ Seq(
      IntegrationTest / javaSource := baseDirectory.value / "src/integration/java",
      IntegrationTest / scalaSource := baseDirectory.value / "src/integration/scala",
      IntegrationTest / resourceDirectory := baseDirectory.value / "src/integration/resources",
      IntegrationTest / parallelExecution := true, // enable parallel execution
      IntegrationTest / testForkedParallel := false, // disable forked parallel execution to avoid duplicate spark context in the same JVM
      IntegrationTest / fork := true,
      IntegrationTest / testGrouping := {
        val tests = (IntegrationTest / definedTests).value
        val forkOptions = ForkOptions()
        val groups = tests.grouped(tests.size / 4 + 1).zipWithIndex.map { case (group, index) =>
          val groupName = s"group-${index + 1}"
          new Tests.Group(
            name = groupName,
            tests = group,
            runPolicy = Tests.SubProcess(
              forkOptions.withRunJVMOptions(forkOptions.runJVMOptions ++
                Seq(s"-Djava.io.tmpdir=${baseDirectory.value}/integ-test/target/tmp/$groupName")))
          )
        }
        groups.toSeq
      }
    )),
    inConfig(AwsIntegrationTest)(Defaults.testSettings ++ Seq(
      AwsIntegrationTest / javaSource := baseDirectory.value / "src/aws-integration/java",
      AwsIntegrationTest / scalaSource := baseDirectory.value / "src/aws-integration/scala",
      AwsIntegrationTest / parallelExecution := true,
      AwsIntegrationTest / fork := true,
    )),
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk" % "1.12.397" % "provided"
        exclude ("com.fasterxml.jackson.core", "jackson-databind"),
      "org.scalactic" %% "scalactic" % "3.2.15",
      "org.scalatest" %% "scalatest" % "3.2.15" % "test",
      "com.stephenn" %% "scalatest-json-jsonassert" % "0.2.5" % "test",
      "org.testcontainers" % "testcontainers" % "1.18.0" % "test",
      "org.apache.iceberg" %% s"iceberg-spark-runtime-$sparkMinorVersion" % icebergVersion % "test",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0" % "test"),
    libraryDependencies ++= deps(sparkVersion),
    Test / fullClasspath ++= Seq((flintSparkIntegration / assembly).value, (pplSparkIntegration / assembly).value,
      (sparkSqlApplication / assembly).value
    ),
    IntegrationTest / dependencyClasspath ++= (Test / dependencyClasspath).value,
    AwsIntegrationTest / dependencyClasspath ++= (Test / dependencyClasspath).value,
    integration := (IntegrationTest / test).value,
    awsIntegration := (AwsIntegrationTest / test).value
  )
lazy val integration = taskKey[Unit]("Run integration tests")
lazy val awsIntegration = taskKey[Unit]("Run AWS integration tests")

lazy val e2etest = (project in file("e2e-test"))
  .dependsOn(flintCommons % "test->package", flintSparkIntegration % "test->package", pplSparkIntegration % "test->package", sparkSqlApplication % "test->package")
  .settings(
    commonSettings,
    name := "e2e-test",
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.15" % "test",
      "org.apache.spark" %% "spark-connect-client-jvm" % "3.5.3" % "test",
      "com.amazonaws" % "aws-java-sdk-s3" % "1.12.568" % "test",
      "com.softwaremill.sttp.client3" %% "core" % "3.10.2" % "test",
      "com.softwaremill.sttp.client3" %% "play2-json" % "3.10.2",
      "com.typesafe.play" %% "play-json" % "2.9.2" % "test",
    ),
    libraryDependencies ++= deps(sparkVersion),
    javaOptions ++= Seq(
      s"-DappJar=${(sparkSqlApplication / assembly).value.getAbsolutePath}",
      s"-DextensionJar=${(flintSparkIntegration / assembly).value.getAbsolutePath}",
      s"-DpplJar=${(pplSparkIntegration / assembly).value.getAbsolutePath}",
    )
  )

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
      "com.amazonaws" % "aws-java-sdk-glue" % "1.12.568" % "provided"
        exclude ("com.fasterxml.jackson.core", "jackson-databind"),
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
