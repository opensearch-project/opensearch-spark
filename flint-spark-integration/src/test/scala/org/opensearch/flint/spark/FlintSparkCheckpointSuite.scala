/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.FlintSuite

class FlintSparkCheckpointSuite extends FlintSuite with Matchers {

  test("exists") {
    withCheckpoint { checkpoint =>
      checkpoint.exists() shouldBe false
      checkpoint.createDirectory()
      checkpoint.exists() shouldBe true
    }
  }

  test("create directory") {
    withTempPath { tempDir =>
      val checkpointDir = new Path(tempDir.getAbsolutePath, "sub/subsub")
      val checkpoint = new FlintSparkCheckpoint(spark, checkpointDir.toString)
      checkpoint.createDirectory()

      tempDir.exists() shouldBe true
    }
  }

  test("create temp file") {
    withCheckpoint { checkpoint =>
      val tempFile = checkpoint.createTempFile()
      tempFile shouldBe defined

      // Close the stream to ensure the file is flushed
      tempFile.get.close()

      // Assert that there is a .tmp file
      listFiles(checkpoint.checkpointLocation)
        .exists(isTempFile) shouldBe true
    }
  }

  test("delete") {
    withCheckpoint { checkpoint =>
      checkpoint.createDirectory()
      checkpoint.delete()
      checkpoint.exists() shouldBe false
    }
  }

  private def withCheckpoint(block: FlintSparkCheckpoint => Unit): Unit = {
    withTempPath { checkpointDir =>
      val checkpoint = new FlintSparkCheckpoint(spark, checkpointDir.getAbsolutePath)
      block(checkpoint)
    }
  }

  private def listFiles(dir: String): Array[FileStatus] = {
    val fs = FileSystem.get(spark.sessionState.newHadoopConf())
    fs.listStatus(new Path(dir))
  }

  private def isTempFile(file: FileStatus): Boolean = {
    file.isFile && file.getPath.getName.endsWith(".tmp")
  }
}
