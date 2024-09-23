/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.common.metadata.{FlintIndexMetadataService, FlintMetadata}
import org.opensearch.flint.core.storage.FlintOpenSearchIndexMetadataService
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.scalatest.matchers.should.Matchers

import org.apache.spark.internal.Logging
import org.apache.spark.sql.flint.config.FlintSparkConf

class FlintSparkIndexDescribeITSuite extends FlintSparkSuite with Matchers {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.covering_sql_test"
  private val testIndexMatch1 = "name_and_age_1"
  private val testIndexMatch2 = "name_and_age_2"
  private val testIndexOther = "address"
  private val testFlintIndexMatch1 =
    FlintSparkCoveringIndex.getFlintIndexName(testIndexMatch1, testTable)
  private val testFlintIndexMatch2 =
    FlintSparkCoveringIndex.getFlintIndexName(testIndexMatch2, testTable)
  private val testFlintIndexOther =
    FlintSparkCoveringIndex.getFlintIndexName(testIndexOther, testTable)

  override def beforeEach(): Unit = {
    super.beforeEach()

    createPartitionedAddressTable(testTable)

    flint
      .coveringIndex()
      .name(testIndexMatch1)
      .onTable(testTable)
      .addIndexColumns("name", "age")
      .create()

    flint
      .coveringIndex()
      .name(testIndexMatch2)
      .onTable(testTable)
      .addIndexColumns("name", "age")
      .create()

    flint
      .coveringIndex()
      .name(testIndexOther)
      .onTable(testTable)
      .addIndexColumns("address")
      .create()
  }

  override def afterEach(): Unit = {
    super.afterEach()

    // Delete all test indices
    deleteTestIndex(testFlintIndexMatch1, testFlintIndexMatch2, testFlintIndexOther)
    sql(s"DROP TABLE $testTable")
  }

  test("describe all indexes matching a pattern") {
    val indexNamePattern = FlintSparkCoveringIndex.getFlintIndexName("name_and_age_*", testTable)
    val indexes = flint.describeIndexes(indexNamePattern)
    indexes should have size 2
    indexes.map(_.name) should contain allOf (testFlintIndexMatch1, testFlintIndexMatch2)
  }

  test(
    "describe all indexes matching a pattern with custom index metadata service implementation without get-by-pattern support") {
    setFlintSparkConf(
      FlintSparkConf.CUSTOM_FLINT_INDEX_METADATA_SERVICE_CLASS,
      classOf[NoGetByPatternSupportIndexMetadataService].getName)
    val testFlint = new FlintSpark(spark)
    val indexNamePattern = FlintSparkCoveringIndex.getFlintIndexName("name_and_age_*", testTable)
    val indexes = testFlint.describeIndexes(indexNamePattern)
    indexes should have size 2
    indexes.map(_.name) should contain allOf (testFlintIndexMatch1, testFlintIndexMatch2)
  }
}

class NoGetByPatternSupportIndexMetadataService extends FlintIndexMetadataService with Logging {

  /**
   * Cannot directly extend FlintOpenSearchIndexMetadataService because
   * FlintIndexMetadataServiceBuilder expects custom implementation takes no arguments in its
   * constructor
   */
  private val indexMetadataService = new FlintOpenSearchIndexMetadataService(
    FlintSparkConf().flintOptions())

  override def supportsGetByIndexPattern(): Boolean = false

  /**
   * Does not match index names by pattern. The input is expected to be a list of full index names
   */
  override def getAllIndexMetadata(indexNames: String*): util.Map[String, FlintMetadata] = {
    logInfo(s"Fetching all Flint index metadata for indexes ${indexNames.mkString(",")}");
    indexNames
      .map(index => index -> getIndexMetadata(index))
      .toMap
      .asJava
  }

  override def getIndexMetadata(indexName: String): FlintMetadata =
    indexMetadataService.getIndexMetadata(indexName)

  override def updateIndexMetadata(indexName: String, metadata: FlintMetadata): Unit =
    indexMetadataService.updateIndexMetadata(indexName, metadata)

  override def deleteIndexMetadata(indexName: String): Unit =
    indexMetadataService.deleteIndexMetadata(indexName)
}
