/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.util.concurrent.ThreadPoolExecutor

import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration.{Duration, MINUTES}
import scala.util.{Failure, Success, Try}

import org.opensearch.flint.core.storage.OpenSearchUpdater

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.FlintJob.createSparkSession
import org.apache.spark.sql.FlintREPL.{executeQuery, logInfo, updateFlintInstanceBeforeShutdown}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.util.ThreadUtils

case class JobOperator(
    sparkConf: SparkConf,
    query: String,
    dataSource: String,
    resultIndex: String,
    streaming: Boolean)
    extends Logging
    with FlintJobExecutor {
  private val spark = createSparkSession(sparkConf)

  // jvm shutdown hook
  sys.addShutdownHook(stop())

  def start(): Unit = {
    val threadPool = ThreadUtils.newDaemonFixedThreadPool(1, "check-create-index")
    implicit val executionContext = ExecutionContext.fromExecutor(threadPool)

    var dataToWrite: Option[DataFrame] = None
    val startTime = System.currentTimeMillis()
    // osClient needs spark session to be created first to get FlintOptions initialized.
    // Otherwise, we will have connection exception from EMR-S to OS.
    val osClient = new OSClient(FlintSparkConf().flintOptions())
    var exceptionThrown = true
    try {
      val futureMappingCheck = Future {
        checkAndCreateIndex(osClient, resultIndex)
      }
      val data = executeQuery(spark, query, dataSource, "", "")

      val mappingCheckResult = ThreadUtils.awaitResult(futureMappingCheck, Duration(1, MINUTES))
      dataToWrite = Some(mappingCheckResult match {
        case Right(_) => data
        case Left(error) =>
          getFailedData(spark, dataSource, error, "", query, "", startTime, currentTimeProvider)
      })
      exceptionThrown = false
    } catch {
      case e: TimeoutException =>
        val error = s"Getting the mapping of index $resultIndex timed out"
        logError(error, e)
        dataToWrite = Some(
          getFailedData(spark, dataSource, error, "", query, "", startTime, currentTimeProvider))
      case e: Exception =>
        val error = processQueryException(e, spark, dataSource, query, "", "")
        dataToWrite = Some(
          getFailedData(spark, dataSource, error, "", query, "", startTime, currentTimeProvider))
    } finally {
      cleanUpResources(exceptionThrown, threadPool, dataToWrite, resultIndex, osClient)
    }
  }

  def cleanUpResources(
      exceptionThrown: Boolean,
      threadPool: ThreadPoolExecutor,
      dataToWrite: Option[DataFrame],
      resultIndex: String,
      osClient: OSClient): Unit = {
    try {
      dataToWrite.foreach(df => writeDataFrameToOpensearch(df, resultIndex, osClient))
    } catch {
      case e: Exception => logError("fail to write to result index", e)
    }

    try {
      // Stop SparkSession if streaming job succeeds
      if (!exceptionThrown && streaming) {
        // wait if any child thread to finish before the main thread terminates
        spark.streams.awaitAnyTermination()
      }
    } catch {
      case e: Exception => logError("streaming job failed", e)
    }

    try {
      threadPool.shutdown()
    } catch {
      case e: Exception => logError("Fail to close threadpool", e)
    }
  }

  def stop(): Unit = {
    Try {
      spark.stop()
    } match {
      case Success(_) =>
      case Failure(e) => logError("unexpected error while shutdown", e)
    }
  }
}
