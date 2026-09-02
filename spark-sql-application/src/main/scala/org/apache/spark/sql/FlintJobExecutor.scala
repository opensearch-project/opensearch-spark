/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.util.Locale
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicInteger

import scala.util.control.NonFatal

import com.amazonaws.services.glue.model.{AccessDeniedException, AWSGlueException}
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.text.StringEscapeUtils.unescapeJava
import org.opensearch.common.Strings
import org.opensearch.flint.common.model.FlintStatement
import org.opensearch.flint.core.IRestHighLevelClient
import org.opensearch.flint.core.logging.{CustomLogging, ExceptionMessages, OperationMessage}
import org.opensearch.flint.core.metrics.MetricConstants
import org.opensearch.flint.core.metrics.MetricsUtil.incrementCounter
import org.opensearch.flint.core.storage.OpenSearchBulkWriteException
import play.api.libs.json._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.FlintREPL.instantiate
import org.apache.spark.sql.SparkConfConstants.{DEFAULT_SQL_EXTENSIONS, SQL_EXTENSIONS_KEY}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.exception.UnrecoverableException
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.config.FlintSparkConf.REFRESH_POLICY
import org.apache.spark.sql.types._
import org.apache.spark.sql.util._
import org.apache.spark.util.Utils

object SparkConfConstants {
  val SQL_EXTENSIONS_KEY = "spark.sql.extensions"
  val DEFAULT_SQL_EXTENSIONS =
    "org.opensearch.flint.spark.FlintPPLSparkExtensions,org.opensearch.flint.spark.FlintSparkExtensions"
}

object FlintJobType {
  val INTERACTIVE = "interactive"
  val BATCH = "batch"
  val STREAMING = "streaming"
}

trait FlintJobExecutor {
  this: Logging =>

  val mapper = new ObjectMapper()
  val throwableHandler = new ThrowableHandler()

  var currentTimeProvider: TimeProvider = new RealTimeProvider()
  var threadPoolFactory: ThreadPoolFactory = new DefaultThreadPoolFactory()
  var environmentProvider: EnvironmentProvider = new RealEnvironment()
  var enableHiveSupport: Boolean = true
  // terminate JVM in the presence non-daemon thread before exiting
  var terminateJVM = true

  // The enabled setting, which can be applied only to the top-level mapping definition and to object fields,
  val resultIndexMapping =
    """{
      "dynamic": false,
      "properties": {
        "result": {
          "type": "object",
          "enabled": false
        },
        "schema": {
          "type": "object",
          "enabled": false
        },
        "jobRunId": {
          "type": "keyword"
        },
        "applicationId": {
          "type": "keyword"
        },
        "dataSourceName": {
          "type": "keyword"
        },
        "status": {
          "type": "keyword"
        },
        "queryId": {
           "type": "keyword"
        },
        "queryText": {
           "type": "text"
        },
        "sessionId": {
           "type": "keyword"
        },
        "jobType": {
           "type": "keyword"
        },
        "updateTime": {
           "type": "date",
           "format": "strict_date_time||epoch_millis"
        },
        "error": {
          "type": "text"
        },
        "queryRunTime" : {
          "type" : "long"
        }
      }
    }""".stripMargin

  // Fast refresh index setting for OpenSearch index. Eliminates index refresh as a source
  // of latency for interactive queries.
  val resultIndexSettings =
    """{
      "index": {
        "refresh_interval": "1s"
      }
    }""".stripMargin

  // Define the data schema
  val schema = StructType(
    Seq(
      StructField("result", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("schema", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("jobRunId", StringType, nullable = true),
      StructField("applicationId", StringType, nullable = true),
      StructField("dataSourceName", StringType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("error", StringType, nullable = true),
      StructField("queryId", StringType, nullable = true),
      StructField("queryText", StringType, nullable = true),
      StructField("sessionId", StringType, nullable = true),
      StructField("jobType", StringType, nullable = true),
      // number is not nullable
      StructField("updateTime", LongType, nullable = false),
      StructField("queryRunTime", LongType, nullable = true)))

  def createSparkConf(): SparkConf = {
    val conf = new SparkConf().setAppName(getClass.getSimpleName)

    if (!conf.contains(SQL_EXTENSIONS_KEY)) {
      conf.set(SQL_EXTENSIONS_KEY, DEFAULT_SQL_EXTENSIONS)
    }

    logInfo(s"Value of $SQL_EXTENSIONS_KEY: ${conf.get(SQL_EXTENSIONS_KEY)}")

    conf
  }

  /*
   * Override dynamicAllocation.maxExecutors with streaming maxExecutors. more detail at
   * https://github.com/opensearch-project/opensearch-spark/issues/324
   */
  def configDYNMaxExecutors(conf: SparkConf, jobType: String): Unit = {
    if (jobType.equalsIgnoreCase(FlintJobType.STREAMING)) {
      conf.set(
        "spark.dynamicAllocation.maxExecutors",
        conf
          .get("spark.flint.streaming.dynamicAllocation.maxExecutors", "10"))
    }
  }

  def createSparkSession(conf: SparkConf): SparkSession = {
    val builder = SparkSession.builder().config(conf)
    if (enableHiveSupport) {
      builder.enableHiveSupport()
    }
    builder.getOrCreate()
  }

  private def writeData(
      resultData: DataFrame,
      resultIndex: String,
      refreshPolicy: String): Unit = {
    try {
      resultData.write
        .format("flint")
        .option(REFRESH_POLICY.optionKey, refreshPolicy)
        .mode("append")
        .save(resultIndex)
      IRestHighLevelClient.recordOperationSuccess(
        MetricConstants.RESULT_METADATA_WRITE_METRIC_PREFIX)
    } catch {
      case t: Throwable =>
        IRestHighLevelClient.recordOperationFailure(
          MetricConstants.RESULT_METADATA_WRITE_METRIC_PREFIX,
          t)
        // Re-throw the exception
        throw t
    }
  }

  /**
   * writes the DataFrame to the specified Elasticsearch index, and createIndex creates an index
   * with the given mapping if it does not exist.
   * @param resultData
   *   data to write
   * @param resultIndex
   *   result index
   * @param osClient
   *   OpenSearch client
   */
  def writeDataFrameToOpensearch(
      resultData: DataFrame,
      resultIndex: String,
      osClient: OSClient): Unit = {
    val refreshPolicy = osClient.flintOptions.getRefreshPolicy;
    if (osClient.doesIndexExist(resultIndex)) {
      writeData(resultData, resultIndex, refreshPolicy)
    } else {
      createResultIndex(osClient, resultIndex, resultIndexMapping, resultIndexSettings)
      writeData(resultData, resultIndex, refreshPolicy)
    }
  }

  // scalastyle:off
  /**
   * Create a new formatted dataframe with json result, json schema and EMR_STEP_ID.
   *
   * @param result
   *   sql query result dataframe
   * @param spark
   *   spark session
   * @return
   *   dataframe with result, schema and emr step id
   */
  def getFormattedData(
      applicationId: String,
      jobId: String,
      result: DataFrame,
      spark: SparkSession,
      dataSource: String,
      queryId: String,
      query: String,
      sessionId: String,
      startTime: Long,
      timeProvider: TimeProvider,
      cleaner: Cleaner): DataFrame = {
    // Create the schema dataframe
    val schemaRows = result.schema.fields.map { field =>
      Row(field.name, field.dataType.typeName)
    }
    val resultSchema = spark.createDataFrame(
      spark.sparkContext.parallelize(schemaRows),
      StructType(
        Seq(
          StructField("column_name", StringType, nullable = false),
          StructField("data_type", StringType, nullable = false))))

    val resultToSave = result.toJSON.collect.toList
      .map(_.replaceAll("'", "\\\\'").replaceAll("\"", "'"))

    val resultSchemaToSave = resultSchema.toJSON.collect.toList.map(_.replaceAll("\"", "'"))
    val endTime = timeProvider.currentEpochMillis()

    // https://github.com/opensearch-project/opensearch-spark/issues/302. Clean shuffle data
    // after consumed the query result. Streaming query shuffle data is cleaned after each
    // microBatch execution.
    cleaner.cleanUp(spark)
    // Create the data rows
    val rows = Seq(
      (
        resultToSave,
        resultSchemaToSave,
        jobId,
        applicationId,
        dataSource,
        "SUCCESS",
        "",
        queryId,
        query,
        sessionId,
        spark.conf.get(FlintSparkConf.JOB_TYPE.key),
        endTime,
        endTime - startTime))

    // Create the DataFrame for data
    spark.createDataFrame(rows).toDF(schema.fields.map(_.name): _*)
  }

  def constructErrorDF(
      applicationId: String,
      jobId: String,
      spark: SparkSession,
      dataSource: String,
      status: String,
      error: String,
      queryId: String,
      queryText: String,
      sessionId: String,
      startTime: Long): DataFrame = {

    val updateTime = currentTimeProvider.currentEpochMillis()

    // Create the data rows
    val rows = Seq(
      (
        null,
        null,
        jobId,
        applicationId,
        dataSource,
        status.toUpperCase(Locale.ROOT),
        error,
        queryId,
        queryText,
        sessionId,
        spark.conf.get(FlintSparkConf.JOB_TYPE.key),
        updateTime,
        updateTime - startTime))

    // Create the DataFrame for data
    spark.createDataFrame(rows).toDF(schema.fields.map(_.name): _*)
  }

  def isSuperset(input: String, mapping: String): Boolean = {

    /**
     * Determines whether one JSON structure is a superset of another.
     *
     * This method checks if the `input` JSON structure contains all the fields and values present
     * in the `mapping` JSON structure. The comparison is recursive and structure-sensitive,
     * ensuring that nested objects and arrays are also compared accurately.
     *
     * Additionally, this method accommodates the edge case where boolean values in the JSON are
     * represented as strings (e.g., "true" or "false" instead of true or false). This is handled
     * by performing a case-insensitive comparison of string representations of boolean values.
     *
     * @param input
     *   The input JSON structure as a String.
     * @param mapping
     *   The mapping JSON structure as a String.
     * @return
     *   A Boolean value indicating whether the `input` JSON structure is a superset of the
     *   `mapping` JSON structure.
     */
    def compareJson(inputJson: JsValue, mappingJson: JsValue): Boolean = {
      (inputJson, mappingJson) match {
        case (JsObject(inputFields), JsObject(mappingFields)) =>
          mappingFields.forall { case (key, value) =>
            inputFields
              .get(key)
              .exists(inputValue => compareJson(inputValue, value))
          }
        case (JsArray(inputValues), JsArray(mappingValues)) =>
          mappingValues.forall(mappingValue =>
            inputValues.exists(inputValue => compareJson(inputValue, mappingValue)))
        case (JsString(inputValue), JsString(mappingValue))
            if (inputValue.toLowerCase(Locale.ROOT) == "true" ||
              inputValue.toLowerCase(Locale.ROOT) == "false") &&
              (mappingValue.toLowerCase(Locale.ROOT) == "true" ||
                mappingValue.toLowerCase(Locale.ROOT) == "false") =>
          inputValue.toLowerCase(Locale.ROOT) == mappingValue.toLowerCase(Locale.ROOT)
        case (JsBoolean(inputValue), JsString(mappingValue))
            if mappingValue.toLowerCase(Locale.ROOT) == "true" ||
              mappingValue.toLowerCase(Locale.ROOT) == "false" =>
          inputValue.toString.toLowerCase(Locale.ROOT) == mappingValue
            .toLowerCase(Locale.ROOT)
        case (JsString(inputValue), JsBoolean(mappingValue))
            if inputValue.toLowerCase(Locale.ROOT) == "true" ||
              inputValue.toLowerCase(Locale.ROOT) == "false" =>
          inputValue.toLowerCase(Locale.ROOT) == mappingValue.toString
            .toLowerCase(Locale.ROOT)
        case (inputValue, mappingValue) =>
          inputValue == mappingValue
      }
    }

    val inputJson = Json.parse(input)
    val mappingJson = Json.parse(mapping)

    compareJson(inputJson, mappingJson) || compareJson(mappingJson, inputJson)
  }

  def checkAndCreateIndex(osClient: OSClient, resultIndex: String): Either[String, Unit] = {
    try {
      val existingSchema = osClient.getIndexMetadata(resultIndex)
      if (!isSuperset(existingSchema, resultIndexMapping)) {
        Left(s"The mapping of $resultIndex is incorrect.")
      } else {
        Right(())
      }
    } catch {
      case e: IllegalStateException
          if e.getCause != null &&
            e.getCause.getMessage.contains("index_not_found_exception") =>
        createResultIndex(osClient, resultIndex, resultIndexMapping, resultIndexSettings)
      case e: InterruptedException =>
        val error = s"Interrupted by the main thread: ${e.getMessage}"
        Thread.currentThread().interrupt() // Preserve the interrupt status
        logError(error, e)
        Left(error)
      case e: Exception =>
        val error = s"Failed to verify existing mapping: ${e.getMessage}"
        logError(error, e)
        Left(error)
    }
  }

  def createResultIndex(
      osClient: OSClient,
      resultIndex: String,
      mapping: String,
      settings: String): Either[String, Unit] = {
    try {
      logInfo(s"create $resultIndex")
      osClient.createIndex(resultIndex, mapping, settings)
      logInfo(s"create $resultIndex successfully")
      Right(())
    } catch {
      case e: Exception =>
        val error = s"Failed to create result index $resultIndex"
        logError(error, e)
        Left(error)
    }
  }

  /**
   * Unescape the query string which is escaped for EMR spark submit parameter parsing. Ref:
   * https://github.com/opensearch-project/sql/pull/2587
   */
  def unescapeQuery(query: String): String = {
    unescapeJava(query)
  }

  def executeQuery(
      applicationId: String,
      jobId: String,
      spark: SparkSession,
      query: String,
      dataSource: String,
      queryId: String,
      sessionId: String,
      streaming: Boolean): DataFrame = {
    // Execute SQL query
    val startTime = System.currentTimeMillis()
    // we have to set job group in the same thread that started the query according to spark doc
    spark.sparkContext.setJobGroup(queryId, "Job group for " + queryId, interruptOnCancel = true)
    val result: DataFrame = spark.sql(query)
    // Get Data
    getFormattedData(
      applicationId,
      jobId,
      result,
      spark,
      dataSource,
      queryId,
      query,
      sessionId,
      startTime,
      currentTimeProvider,
      CleanerFactory.cleaner(streaming))
  }

  /** Persisted/forwarded message. See [[ErrorSanitizer.customerMessage]]. */
  private[sql] def customerMessage(t: Throwable): String = ErrorSanitizer.customerMessage(t)

  /** Driver-log message. See [[ErrorSanitizer.operatorLogMessage]]. */
  private[sql] def operatorLogMessage(t: Throwable): String = ErrorSanitizer.operatorLogMessage(t)

  /** Log-safe throwable wrapper. See [[ErrorSanitizer.redactThrowable]]. */
  private[sql] def redactThrowable(t: Throwable): Throwable = ErrorSanitizer.redactThrowable(t)

  private def handleQueryException(
      t: Throwable,
      messagePrefix: String,
      errorSource: Option[String] = None,
      statusCode: Option[Int] = None,
      // Wrapper used for strict operator classification; defaults to the persisted root cause.
      originalThrowable: Throwable = null): String = {
    throwableHandler.setThrowable(t)

    // Keep wrappers so a task-failure SparkException cannot fall through to a raw-message floor.
    val logThrowable = if (originalThrowable == null) t else originalThrowable

    // Persist one actionable line; log only strict operator-safe classification and templates.
    val persistedMessage = s"$messagePrefix: ${customerMessage(t)}"
    val logMessage = s"$messagePrefix: ${operatorLogMessage(logThrowable)}"
    val logSafeThrowable = redactThrowable(logThrowable)
    val classification = ErrorSanitizer.classify(t)
    // Compute one status so persisted and operator records agree.
    val effectiveStatusCode: Option[Int] = statusCode.orElse(classification.statusCode)
    val errorDetails = new java.util.LinkedHashMap[String, String]()
    errorDetails.put("message", persistedMessage)
    errorSource.foreach(es => errorDetails.put("ErrorSource", es))
    effectiveStatusCode.foreach(code => errorDetails.put("statusCode", code.toString))
    errorDetails.put("errorCode", classification.errorCode)
    // Compatibility shim: legacy authorization rules require RuntimeException plus the canonical
    // message token. Remove after downstream consumers use errorCode/statusCode.
    val persistedExceptionType = t match {
      case _: OpenSearchBulkWriteException => classOf[RuntimeException].getName
      case _ => t.getClass.getName
    }
    errorDetails.put("exception.type", persistedExceptionType)

    val errorJson = mapper.writeValueAsString(errorDetails)
    throwableHandler.setError(errorJson)

    val operatorContext = ErrorSanitizer.operatorLogContext(logThrowable)
    val operationMessage = new OperationMessage(
      logMessage,
      effectiveStatusCode.map(Int.box).orNull,
      classification.errorCode,
      logThrowable.getClass.getName,
      operatorContext.requestId.orNull,
      operatorContext.extendedRequestId.orNull)
    CustomLogging.logError(operationMessage, logSafeThrowable)

    errorJson
  }

  def getRootCause(t: Throwable): Throwable = {
    // Walk to the deepest cause, guarding against a cyclic chain (a throwable whose cause is
    // itself, or a longer loop). This runs on the error-handling path, where a naive recursion
    // would StackOverflow on such a chain and mask the original failure with a secondary error.
    // Tail-recursive, so it compiles to a loop and does not grow the stack. Mirrors the
    // cycle-safety already relied on by ErrorSanitizer.classify.
    @scala.annotation.tailrec
    def loop(current: Throwable, seen: Set[Throwable]): Throwable = {
      // getCause runs on the error-handling path; a hostile throwable whose getCause() throws
      // would otherwise propagate a secondary failure out of processQueryException and mask the
      // original query error. Treat a throwing getCause() as "no further cause" and stop here.
      val cause =
        try current.getCause
        catch { case NonFatal(_) => null }
      if (cause == null || seen.contains(cause)) current
      else loop(cause, seen + current)
    }
    loop(t, Set.empty)
  }

  /**
   * This method converts query exception into error string, which then persist to query result
   * metadata
   */
  def processQueryException(throwable: Throwable): String = {
    // Classification, the customer message, and the persisted exception.type are built from the
    // unwrapped root cause below. The operator log, in contrast, is handed the original `throwable`
    // (via `originalThrowable`) so an enclosing wrapper is still visible to the strict redaction and
    // a wrapped non-Spark cause cannot reach the raw first-line log floor.
    getRootCause(throwable) match {
      case r: ParseException =>
        handleQueryException(
          r,
          ExceptionMessages.SyntaxErrorPrefix,
          originalThrowable = throwable)
      case r: AmazonS3Exception =>
        incrementCounter(MetricConstants.S3_ERR_CNT_METRIC)
        handleQueryException(
          r,
          ExceptionMessages.S3ErrorPrefix,
          Some(r.getServiceName),
          Some(r.getStatusCode),
          originalThrowable = throwable)
      case r: AWSGlueException =>
        incrementCounter(MetricConstants.GLUE_ERR_CNT_METRIC)
        // Redact Access denied in AWS Glue service
        r match {
          case accessDenied: AccessDeniedException =>
            accessDenied.setErrorMessage(ExceptionMessages.GlueAccessDeniedMessage)
          case _ => // No additional action for other types of AWSGlueException
        }
        handleQueryException(
          r,
          ExceptionMessages.GlueErrorPrefix,
          Some(r.getServiceName),
          Some(r.getStatusCode),
          originalThrowable = throwable)
      case r: AnalysisException =>
        handleQueryException(
          r,
          ExceptionMessages.QueryAnalysisErrorPrefix,
          originalThrowable = throwable)
      case r: SparkException =>
        handleQueryException(
          r,
          ExceptionMessages.SparkExceptionErrorPrefix,
          originalThrowable = throwable)
      case t: Throwable =>
        val rootCauseClassName = t.getClass.getName
        // Read the message defensively: this is the error-handling path, so a hostile throwable
        // whose getMessage() throws must not propagate a secondary failure, and a MetaException
        // with a null message must not NPE on the contains(...) check below (the MetaException
        // class-name guard alone does not short-circuit a null errMsg). Fall back to "" in both
        // cases, which routes to the generic redaction path.
        val errMsg =
          try Option(t.getMessage).getOrElse("")
          catch { case NonFatal(_) => "" }
        if (rootCauseClassName == "org.apache.hadoop.hive.metastore.api.MetaException" &&
          errMsg.contains("com.amazonaws.services.glue.model.AccessDeniedException")) {
          // A curated SecurityException is substituted deliberately; its own message is the safe,
          // actionable sentence for both channels, so the original MetaException wrapper is NOT
          // forwarded to the operator log (that would surface the raw metastore text).
          val e = new SecurityException(ExceptionMessages.GlueAccessDeniedMessage)
          handleQueryException(e, ExceptionMessages.QueryRunErrorPrefix)
        } else {
          handleQueryException(
            t,
            ExceptionMessages.QueryRunErrorPrefix,
            originalThrowable = throwable)
        }
    }
  }

  /**
   * Before OS 2.13, there are two arguments from entry point: query and result index Starting
   * from OS 2.13, query is optional for FlintREPL And since Flint 0.5, result index is also
   * optional for non-OpenSearch result persist
   */
  def parseArgs(args: Array[String]): (Option[String], Option[String]) = {
    args match {
      case Array() =>
        (None, None)
      case Array(resultIndex) =>
        (None, Some(resultIndex))
      case Array(query, resultIndex) =>
        (Some(query), Some(resultIndex))
      case _ =>
        logAndThrow("Unsupported number of arguments. Expected no more than two arguments.")
    }
  }

  def logAndThrow(errorMessage: String): Nothing = {
    val t = new IllegalArgumentException(errorMessage)
    CustomLogging.logError(t)
    throw t
  }

  def checkAndThrowUnrecoverableExceptions(): Unit = {
    throwableHandler.exceptionThrown.foreach {
      case e: UnrecoverableException =>
        throw e
      case _ => // Do nothing for other types of exceptions
    }
  }

  def instantiate[T](defaultConstructor: => T, className: String, args: Any*): T = {
    if (Strings.isNullOrEmpty(className)) {
      defaultConstructor
    } else {
      try {
        val classObject = Utils.classForName(className)
        val ctor = if (args.isEmpty) {
          classObject.getDeclaredConstructor()
        } else {
          classObject.getDeclaredConstructor(args.map(_.getClass.asInstanceOf[Class[_]]): _*)
        }
        ctor.setAccessible(true)
        ctor.newInstance(args.map(_.asInstanceOf[Object]): _*).asInstanceOf[T]
      } catch {
        case e: Exception =>
          throw new RuntimeException(s"Failed to instantiate provider: $className", e)
      }
    }
  }

  def createJobOperator(
      spark: SparkSession,
      applicationId: String,
      jobId: String,
      flintStatement: FlintStatement,
      dataSource: String,
      resultIndex: String,
      jobType: String,
      streamingRunningCount: AtomicInteger,
      statementRunningCount: AtomicInteger): JobOperator = {
    // https://github.com/opensearch-project/opensearch-spark/issues/138
    /*
     * To execute queries such as `CREATE SKIPPING INDEX ON my_glue1.default.http_logs_plain (`@timestamp` VALUE_SET) WITH (auto_refresh = true)`,
     * it's necessary to set `spark.sql.defaultCatalog=my_glue1`. This is because AWS Glue uses a single database (default) and table (http_logs_plain),
     * and we need to configure Spark to recognize `my_glue1` as a reference to AWS Glue's database and table.
     * By doing this, we effectively map `my_glue1` to AWS Glue, allowing Spark to resolve the database and table names correctly.
     * Without this setup, Spark would not recognize names in the format `my_glue1.default`.
     */
    spark.conf.set("spark.sql.defaultCatalog", dataSource)
    val jobOperator =
      JobOperator(
        applicationId,
        jobId,
        spark,
        flintStatement,
        dataSource,
        resultIndex,
        jobType,
        streamingRunningCount,
        statementRunningCount)
    jobOperator
  }

  def instantiateQueryResultWriter(
      spark: SparkSession,
      commandContext: CommandContext): QueryResultWriter = {
    instantiate(
      new QueryResultWriterImpl(commandContext),
      spark.conf.get(FlintSparkConf.CUSTOM_QUERY_RESULT_WRITER.key, ""))
  }

  def instantiateStatementExecutionManager(
      commandContext: CommandContext): StatementExecutionManager = {
    import commandContext._
    instantiate(
      new StatementExecutionManagerImpl(commandContext),
      spark.conf.get(FlintSparkConf.CUSTOM_STATEMENT_MANAGER.key, ""),
      spark,
      sessionId)
  }

  def instantiateSessionManager(
      spark: SparkSession,
      resultIndexOption: Option[String]): SessionManager = {
    instantiate(
      new SessionManagerImpl(spark, resultIndexOption),
      spark.conf.get(FlintSparkConf.CUSTOM_SESSION_MANAGER.key, ""),
      resultIndexOption.getOrElse(""))
  }
}
