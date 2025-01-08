/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark

import org.opensearch.flint.spark.FlintSparkExtensions
import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{Alias, CodegenObjectFactoryMode, Expression}
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.flint.config.{FlintConfigEntry, FlintSparkConf}
import org.apache.spark.sql.flint.config.FlintSparkConf.{EXTERNAL_SCHEDULER_ENABLED, HYBRID_SCAN_ENABLED, METADATA_CACHE_WRITE}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.sql.test.SharedSparkSession

trait FlintSuite extends SharedSparkSession {
  override protected def sparkConf = {
    val conf = new SparkConf()
      .set("spark.ui.enabled", "false")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.CODEGEN_ONLY.toString)
      // Disable ConvertToLocalRelation for better test coverage. Test cases built on
      // LocalRelation will exercise the optimization rules better by disabling it as
      // this rule may potentially block testing of other optimization rules such as
      // ConstantPropagation etc.
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
      .set("spark.sql.extensions", classOf[FlintSparkExtensions].getName)
      // Override scheduler class for unit testing
      .set(
        FlintSparkConf.CUSTOM_FLINT_SCHEDULER_CLASS.key,
        "org.opensearch.flint.core.scheduler.AsyncQuerySchedulerBuilderTest$AsyncQuerySchedulerForLocalTest")
      .set(WAREHOUSE_PATH.key, s"spark-warehouse/${suiteName}")
    conf
  }

  /**
   * Set Flint Spark configuration. (Generic "value: T" has problem with FlintConfigEntry[Any])
   */
  protected def setFlintSparkConf[T](config: FlintConfigEntry[T], value: Any): Unit = {
    spark.conf.set(config.key, value.toString)
  }

  protected def withHybridScanEnabled(block: => Unit): Unit = {
    setFlintSparkConf(HYBRID_SCAN_ENABLED, "true")
    try {
      block
    } finally {
      setFlintSparkConf(HYBRID_SCAN_ENABLED, "false")
    }
  }

  protected def withExternalSchedulerEnabled(block: => Unit): Unit = {
    setFlintSparkConf(EXTERNAL_SCHEDULER_ENABLED, "true")
    try {
      block
    } finally {
      setFlintSparkConf(EXTERNAL_SCHEDULER_ENABLED, "false")
    }
  }

  protected def withMetadataCacheWriteEnabled(block: => Unit): Unit = {
    setFlintSparkConf(METADATA_CACHE_WRITE, "true")
    try {
      block
    } finally {
      setFlintSparkConf(METADATA_CACHE_WRITE, "false")
    }
  }

  /**
   * Implicit class to extend DataFrame functionality with additional utilities.
   *
   * @param df
   *   the DataFrame to which the additional methods are added
   */
  protected implicit class DataFrameExtensions(val df: DataFrame) {

    /**
     * Retrieves the ID column expression from the logical plan of the DataFrame, if it exists.
     *
     * @return
     *   an `Option` containing the `Expression` for the ID column if present, or `None` otherwise
     */
    def idColumn(): Option[Expression] = {
      df.queryExecution.logical.collectFirst { case Project(projectList, _) =>
        projectList.collectFirst { case Alias(child, ID_COLUMN) =>
          child
        }
      }.flatten
    }
  }
}
