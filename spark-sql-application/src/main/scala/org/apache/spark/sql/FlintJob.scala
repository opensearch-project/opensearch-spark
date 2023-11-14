/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

// defined in spark package so that I can use ThreadUtils
package org.apache.spark.sql

import java.util.Locale

import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration.{Duration, MINUTES}

import org.opensearch.client.{RequestOptions, RestHighLevelClient}
import org.opensearch.cluster.metadata.MappingMetadata
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder, FlintOptions}
import org.opensearch.flint.core.metadata.FlintMetadata
import play.api.libs.json._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.util.ThreadUtils

/**
 * Spark SQL Application entrypoint
 *
 * @param args
 *   (0) sql query
 * @param args
 *   (1) opensearch index name
 * @return
 *   write sql query result to given opensearch index
 */
object FlintJob extends Logging with FlintJobExecutor {
  def main(args: Array[String]): Unit = {
    // Validate command line arguments
    if (args.length != 2) {
      throw new IllegalArgumentException("Usage: FlintJob <query> <resultIndex>")
    }

    val Array(query, resultIndex) = args

    val conf = createSparkConf()
    val wait = conf.get("spark.flint.job.type", "continue")
    val dataSource = conf.get("spark.flint.datasource.name", "")
    val spark = createSparkSession(conf)

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
      dataToWrite.foreach(df => writeDataFrameToOpensearch(df, resultIndex, osClient))
      // Stop SparkSession if streaming job succeeds
      if (!exceptionThrown && wait.equalsIgnoreCase("streaming")) {
        // wait if any child thread to finish before the main thread terminates
        spark.streams.awaitAnyTermination()
      } else {
        spark.stop()
      }

      threadPool.shutdown()
    }
  }
}
