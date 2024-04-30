/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.config

import java.util
import java.util.{Map => JMap, NoSuchElementException}

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.http.FlintRetryOptions

import org.apache.spark.internal.config.ConfigReader
import org.apache.spark.sql.flint.config.FlintSparkConf._
import org.apache.spark.sql.internal.SQLConf

/**
 * Define all the Flint Spark Related configuration. <p> User define the config as xxx.yyy using
 * {@link FlintConfig}.
 *
 * <p> How to use config <ol> <li> define config using spark.datasource.flint.xxx.yyy in spark
 * conf. <li> define config using xxx.yyy in datasource options. <li> Configurations defined in
 * the datasource options will override the same configurations present in the Spark
 * configuration. </ol>
 */
object FlintSparkConf {

  val DATASOURCE_FLINT_PREFIX = "spark.datasource.flint."

  /**
   * Create FlintSparkConf from Datasource options. if no options provided, FlintSparkConf will
   * read configuraiton from SQLConf.
   */
  def apply(options: JMap[String, String] = new util.HashMap[String, String]()): FlintSparkConf =
    new FlintSparkConf(options)

  val HOST_ENDPOINT = FlintConfig("spark.datasource.flint.host")
    .datasourceOption()
    .createWithDefault("localhost")

  val HOST_PORT = FlintConfig("spark.datasource.flint.port")
    .datasourceOption()
    .createWithDefault("9200")

  val SCHEME = FlintConfig("spark.datasource.flint.scheme")
    .datasourceOption()
    .doc("http or https")
    .createWithDefault("http")

  val AUTH = FlintConfig("spark.datasource.flint.auth")
    .datasourceOption()
    .doc("authentication type. supported value: " +
      "noauth(no auth), sigv4(sigv4 auth), basic(basic auth)")
    .createWithDefault(FlintOptions.NONE_AUTH)

  val USERNAME = FlintConfig("spark.datasource.flint.auth.username")
    .datasourceOption()
    .doc("basic auth username")
    .createWithDefault("flint")

  val PASSWORD = FlintConfig("spark.datasource.flint.auth.password")
    .datasourceOption()
    .doc("basic auth password")
    .createWithDefault("flint")

  val REGION = FlintConfig("spark.datasource.flint.region")
    .datasourceOption()
    .doc("AWS service region")
    .createWithDefault(FlintOptions.DEFAULT_REGION)

  val CUSTOM_AWS_CREDENTIALS_PROVIDER =
    FlintConfig("spark.datasource.flint.customAWSCredentialsProvider")
      .datasourceOption()
      .doc("AWS customAWSCredentialsProvider")
      .createWithDefault(FlintOptions.DEFAULT_AWS_CREDENTIALS_PROVIDER)

  val DOC_ID_COLUMN_NAME = FlintConfig("spark.datasource.flint.write.id_name")
    .datasourceOption()
    .doc(
      "spark write task use spark.flint.write.id.name defined column as doc id when write to " +
        "flint. if not provided, use system generated random id")
    .createOptional()

  val IGNORE_DOC_ID_COLUMN = FlintConfig("spark.datasource.flint.ignore.id_column")
    .datasourceOption()
    .doc("Enable spark write task ignore doc_id column. the default value is ture")
    .createWithDefault("true")

  val BATCH_SIZE = FlintConfig("spark.datasource.flint.write.batch_size")
    .datasourceOption()
    .doc(
      "The number of documents written to Flint in a single batch request is determined by the " +
        "overall size of the HTTP request, which should not exceed 100MB. The actual number of " +
        "documents will vary depending on the individual size of each document.")
    .createWithDefault("1000")

  val REFRESH_POLICY = FlintConfig("spark.datasource.flint.write.refresh_policy")
    .datasourceOption()
    .doc("refresh_policy, possible value are NONE(false), IMMEDIATE(true), WAIT_UNTIL(wait_for)")
    .createWithDefault(FlintOptions.DEFAULT_REFRESH_POLICY)

  val SCROLL_SIZE = FlintConfig("spark.datasource.flint.read.scroll_size")
    .datasourceOption()
    .doc("scroll read size")
    .createWithDefault("100")

  val SCROLL_DURATION = FlintConfig(s"spark.datasource.flint.${FlintOptions.SCROLL_DURATION}")
    .datasourceOption()
    .doc("scroll duration in minutes")
    .createWithDefault(String.valueOf(FlintOptions.DEFAULT_SCROLL_DURATION))

  val MAX_RETRIES = FlintConfig(s"spark.datasource.flint.${FlintRetryOptions.MAX_RETRIES}")
    .datasourceOption()
    .doc("max retries on failed HTTP request, 0 means retry is disabled, default is 3")
    .createWithDefault(String.valueOf(FlintRetryOptions.DEFAULT_MAX_RETRIES))

  val RETRYABLE_HTTP_STATUS_CODES =
    FlintConfig(s"spark.datasource.flint.${FlintRetryOptions.RETRYABLE_HTTP_STATUS_CODES}")
      .datasourceOption()
      .doc("retryable HTTP response status code list")
      .createWithDefault(FlintRetryOptions.DEFAULT_RETRYABLE_HTTP_STATUS_CODES)

  val RETRYABLE_EXCEPTION_CLASS_NAMES =
    FlintConfig(s"spark.datasource.flint.${FlintRetryOptions.RETRYABLE_EXCEPTION_CLASS_NAMES}")
      .datasourceOption()
      .doc("retryable exception class name list, by default no retry on exception thrown")
      .createOptional()

  val OPTIMIZER_RULE_ENABLED = FlintConfig("spark.flint.optimizer.enabled")
    .doc("Enable Flint optimizer rule for query rewrite with Flint index")
    .createWithDefault("true")

  val OPTIMIZER_RULE_COVERING_INDEX_ENABLED =
    FlintConfig("spark.flint.optimizer.covering.enabled")
      .doc("Enable Flint optimizer rule for query rewrite with Flint covering index")
      .createWithDefault("true")

  val HYBRID_SCAN_ENABLED = FlintConfig("spark.flint.index.hybridscan.enabled")
    .doc("Enable hybrid scan to include latest source data not refreshed to index yet")
    .createWithDefault("false")

  val CHECKPOINT_MANDATORY = FlintConfig("spark.flint.index.checkpoint.mandatory")
    .doc("Checkpoint location for incremental refresh index will be mandatory if enabled")
    .createWithDefault("true")

  val SOCKET_TIMEOUT_MILLIS =
    FlintConfig(s"spark.datasource.flint.${FlintOptions.SOCKET_TIMEOUT_MILLIS}")
      .datasourceOption()
      .doc("socket duration in milliseconds")
      .createWithDefault(String.valueOf(FlintOptions.DEFAULT_SOCKET_TIMEOUT_MILLIS))
  val DATA_SOURCE_NAME =
    FlintConfig(s"spark.flint.datasource.name")
      .doc("data source name")
      .createOptional()
  val QUERY =
    FlintConfig("spark.flint.job.query")
      .doc("Flint query for batch and streaming job")
      .createOptional()
  val JOB_TYPE =
    FlintConfig(s"spark.flint.job.type")
      .doc("Flint job type. Including interactive and streaming")
      .createWithDefault("interactive")
  val SESSION_ID =
    FlintConfig(s"spark.flint.job.sessionId")
      .doc("Flint session id")
      .createOptional()
  val REQUEST_INDEX =
    FlintConfig(s"spark.flint.job.requestIndex")
      .doc("Request index")
      .createOptional()
  val EXCLUDE_JOB_IDS =
    FlintConfig(s"spark.flint.deployment.excludeJobs")
      .doc("Exclude job ids")
      .createOptional()
  val REPL_INACTIVITY_TIMEOUT_MILLIS =
    FlintConfig(s"spark.flint.job.inactivityLimitMillis")
      .doc("inactivity timeout")
      .createWithDefault(String.valueOf(FlintOptions.DEFAULT_INACTIVITY_LIMIT_MILLIS))
  val METADATA_ACCESS_AWS_CREDENTIALS_PROVIDER =
    FlintConfig("spark.metadata.accessAWSCredentialsProvider")
      .doc("AWS credentials provider for metadata access permission")
      .createOptional()
}

/**
 * if no options provided, FlintSparkConf read configuration from SQLConf.
 */
case class FlintSparkConf(properties: JMap[String, String]) extends Serializable {

  @transient lazy val reader = new ConfigReader(properties)

  def batchSize(): Int = BATCH_SIZE.readFrom(reader).toInt

  def docIdColumnName(): Option[String] = DOC_ID_COLUMN_NAME.readFrom(reader)

  def ignoreIdColumn(): Boolean = IGNORE_DOC_ID_COLUMN.readFrom(reader).toBoolean

  def tableName(): String = {
    if (properties.containsKey("path")) properties.get("path")
    else throw new NoSuchElementException("index or path not found")
  }

  def isOptimizerEnabled: Boolean = OPTIMIZER_RULE_ENABLED.readFrom(reader).toBoolean

  def isCoveringIndexOptimizerEnabled: Boolean =
    OPTIMIZER_RULE_COVERING_INDEX_ENABLED.readFrom(reader).toBoolean

  def isHybridScanEnabled: Boolean = HYBRID_SCAN_ENABLED.readFrom(reader).toBoolean

  def isCheckpointMandatory: Boolean = CHECKPOINT_MANDATORY.readFrom(reader).toBoolean

  /**
   * spark.sql.session.timeZone
   */
  def timeZone: String = SQLConf.get.sessionLocalTimeZone

  /**
   * Helper class, create {@link FlintOptions}.
   */
  def flintOptions(): FlintOptions = {
    val optionsWithDefault = Seq(
      HOST_ENDPOINT,
      HOST_PORT,
      REFRESH_POLICY,
      SCROLL_SIZE,
      SCROLL_DURATION,
      SCHEME,
      AUTH,
      MAX_RETRIES,
      RETRYABLE_HTTP_STATUS_CODES,
      REGION,
      CUSTOM_AWS_CREDENTIALS_PROVIDER,
      USERNAME,
      PASSWORD,
      SOCKET_TIMEOUT_MILLIS,
      JOB_TYPE,
      REPL_INACTIVITY_TIMEOUT_MILLIS)
      .map(conf => (conf.optionKey, conf.readFrom(reader)))
      .toMap

    val optionsWithoutDefault = Seq(
      RETRYABLE_EXCEPTION_CLASS_NAMES,
      DATA_SOURCE_NAME,
      SESSION_ID,
      REQUEST_INDEX,
      METADATA_ACCESS_AWS_CREDENTIALS_PROVIDER,
      EXCLUDE_JOB_IDS)
      .map(conf => (conf.optionKey, conf.readFrom(reader)))
      .flatMap {
        case (_, None) => None
        case (key, value) => Some(key, value.get)
      }
      .toMap

    new FlintOptions((optionsWithDefault ++ optionsWithoutDefault).asJava)
  }
}
