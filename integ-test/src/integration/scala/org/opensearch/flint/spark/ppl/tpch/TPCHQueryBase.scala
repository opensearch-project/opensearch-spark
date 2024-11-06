/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.tpch

import org.opensearch.flint.spark.ppl.FlintPPLSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.codegen.{ByteCodeStats, CodeFormatter, CodeGenerator}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND
import org.apache.spark.sql.execution.{SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.internal.SQLConf

trait TPCHQueryBase extends FlintPPLSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.MAX_TO_STRING_FIELDS.key, Int.MaxValue.toString)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    RuleExecutor.resetMetrics()
    CodeGenerator.resetCompileTime()
    WholeStageCodegenExec.resetCodeGenTime()
    tpchCreateTable.values.foreach { ppl =>
      sql(ppl)
    }
  }

  override def afterAll(): Unit = {
    try {
      tpchCreateTable.keys.foreach { tableName =>
        spark.sessionState.catalog.dropTable(TableIdentifier(tableName), true, true)
      }
      // For debugging dump some statistics about how much time was spent in various optimizer rules
      // code generation, and compilation.
      logWarning(RuleExecutor.dumpTimeSpent())
      val codeGenTime = WholeStageCodegenExec.codeGenTime.toDouble / NANOS_PER_SECOND
      val compileTime = CodeGenerator.compileTime.toDouble / NANOS_PER_SECOND
      val codegenInfo =
        s"""
           |=== Metrics of Whole-stage Codegen ===
           |Total code generation time: $codeGenTime seconds
           |Total compile time: $compileTime seconds
         """.stripMargin
      logWarning(codegenInfo)
      spark.sessionState.catalog.reset()
    } finally {
      super.afterAll()
    }
  }

  def checkGeneratedCode(plan: SparkPlan, checkMethodCodeSize: Boolean = true): Unit = {
    val codegenSubtrees = new collection.mutable.HashSet[WholeStageCodegenExec]()

    def findSubtrees(plan: SparkPlan): Unit = {
      plan foreach {
        case s: WholeStageCodegenExec =>
          codegenSubtrees += s
        case s =>
          s.subqueries.foreach(findSubtrees)
      }
    }

    findSubtrees(plan)
    codegenSubtrees.toSeq.foreach { subtree =>
      val code = subtree.doCodeGen()._2
      val (_, ByteCodeStats(maxMethodCodeSize, _, _)) =
        try {
          // Just check the generated code can be properly compiled
          CodeGenerator.compile(code)
        } catch {
          case e: Exception =>
            val msg =
              s"""
               |failed to compile:
               |Subtree:
               |$subtree
               |Generated code:
               |${CodeFormatter.format(code)}
             """.stripMargin
            throw new Exception(msg, e)
        }

      assert(
        !checkMethodCodeSize ||
          maxMethodCodeSize <= CodeGenerator.DEFAULT_JVM_HUGE_METHOD_LIMIT,
        s"too long generated codes found in the WholeStageCodegenExec subtree (id=${subtree.id}) " +
          s"and JIT optimization might not work:\n${subtree.treeString}")
    }
  }

  val tpchCreateTable = Map(
    "orders" ->
      """
        |CREATE TABLE `orders` (
        |`o_orderkey` BIGINT, `o_custkey` BIGINT, `o_orderstatus` STRING,
        |`o_totalprice` DECIMAL(10,0), `o_orderdate` DATE, `o_orderpriority` STRING,
        |`o_clerk` STRING, `o_shippriority` INT, `o_comment` STRING)
        |USING parquet
      """.stripMargin,
    "nation" ->
      """
        |CREATE TABLE `nation` (
        |`n_nationkey` BIGINT, `n_name` STRING, `n_regionkey` BIGINT, `n_comment` STRING)
        |USING parquet
      """.stripMargin,
    "region" ->
      """
        |CREATE TABLE `region` (
        |`r_regionkey` BIGINT, `r_name` STRING, `r_comment` STRING)
        |USING parquet
      """.stripMargin,
    "part" ->
      """
        |CREATE TABLE `part` (`p_partkey` BIGINT, `p_name` STRING, `p_mfgr` STRING,
        |`p_brand` STRING, `p_type` STRING, `p_size` INT, `p_container` STRING,
        |`p_retailprice` DECIMAL(10,0), `p_comment` STRING)
        |USING parquet
      """.stripMargin,
    "partsupp" ->
      """
        |CREATE TABLE `partsupp` (`ps_partkey` BIGINT, `ps_suppkey` BIGINT,
        |`ps_availqty` INT, `ps_supplycost` DECIMAL(10,0), `ps_comment` STRING)
        |USING parquet
      """.stripMargin,
    "customer" ->
      """
        |CREATE TABLE `customer` (`c_custkey` BIGINT, `c_name` STRING, `c_address` STRING,
        |`c_nationkey` BIGINT, `c_phone` STRING, `c_acctbal` DECIMAL(10,0),
        |`c_mktsegment` STRING, `c_comment` STRING)
        |USING parquet
      """.stripMargin,
    "supplier" ->
      """
        |CREATE TABLE `supplier` (`s_suppkey` BIGINT, `s_name` STRING, `s_address` STRING,
        |`s_nationkey` BIGINT, `s_phone` STRING, `s_acctbal` DECIMAL(10,0), `s_comment` STRING)
        |USING parquet
      """.stripMargin,
    "lineitem" ->
      """
        |CREATE TABLE `lineitem` (`l_orderkey` BIGINT, `l_partkey` BIGINT, `l_suppkey` BIGINT,
        |`l_linenumber` INT, `l_quantity` DECIMAL(10,0), `l_extendedprice` DECIMAL(10,0),
        |`l_discount` DECIMAL(10,0), `l_tax` DECIMAL(10,0), `l_returnflag` STRING,
        |`l_linestatus` STRING, `l_shipdate` DATE, `l_commitdate` DATE, `l_receiptdate` DATE,
        |`l_shipinstruct` STRING, `l_shipmode` STRING, `l_comment` STRING)
        |USING parquet
      """.stripMargin)

  val tpchQueries = Seq(
    "q1",
    "q2",
    "q3",
    "q4",
    "q5",
    "q6",
    "q7",
    "q8",
    "q9",
    "q10",
    "q11",
    "q12",
    "q13",
    "q14",
    "q15",
    "q16",
    "q17",
    "q18",
    "q19",
    "q20",
    "q21",
    "q22")
}
