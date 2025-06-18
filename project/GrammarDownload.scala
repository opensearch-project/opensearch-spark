/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
import java.net.URL
import java.util.Locale
import java.util.zip.ZipFile

import sbt._
import sbt.Keys._
import scala.collection.JavaConverters._
import scala.xml.XML

object GrammarDownload {
  // Define task keys for grammar extraction
  val downloadPplGrammar = taskKey[Seq[File]]("Download and extract PPL grammar files")
  val downloadFlintGrammar = taskKey[Seq[File]]("Download and extract Flint grammar files")

  // Add resolver for grammar downloads
  val grammarResolvers: Seq[MavenRepository] = Seq(
    "AWS OSS Sonatype Snapshots" at "https://aws.oss.sonatype.org/content/repositories/snapshots"
  )

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

  // Helper function to download grammar files - consolidated functionality
  def downloadGrammar(
                       log: Logger,
                       baseDir: File,
                       grammarFiles: Seq[String],
                       artifactId: String = "language-grammar",
                       version: String = "0.1.0-SNAPSHOT"
                     ): Seq[File] = {
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
  }
}
