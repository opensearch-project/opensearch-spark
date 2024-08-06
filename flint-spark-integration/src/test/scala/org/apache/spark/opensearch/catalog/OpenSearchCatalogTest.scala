/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.catalog

import scala.collection.JavaConverters._

import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.FlintSuite
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Identifier, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.flint.FlintReadOnlyTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class OpenSearchCatalogTest
    extends FlintSuite
    with Matchers
    with BeforeAndAfterEach
    with MockitoSugar {

  private var catalog: OpenSearchCatalog = _
  private val catalogName = "dev"
  private val optionsMap = Map(
    "opensearch.port" -> "9200",
    "opensearch.scheme" -> "http",
    "opensearch.auth" -> "noauth",
    "some.other.config" -> "value")
  private val options = new CaseInsensitiveStringMap(optionsMap.asJava)

  override def beforeEach(): Unit = {
    catalog = new OpenSearchCatalog()
    catalog.initialize(catalogName, options)
  }

  test("Catalog should initialize with given name and options") {
    catalog.name() should be(catalogName)
  }

  test("listTables should throw UnsupportedOperationException") {
    val namespace = Array("default")

    intercept[UnsupportedOperationException] {
      catalog.listTables(namespace)
    }
  }

  test("loadTable should throw NoSuchTableException if namespace is not default") {
    val identifier = mock[Identifier]
    when(identifier.namespace()).thenReturn(Array("non-default"))
    when(identifier.name()).thenReturn("table1")

    intercept[NoSuchTableException] {
      catalog.loadTable(identifier)
    }
  }

  test("loadTable should load table if namespace is default") {
    val identifier = mock[Identifier]
    when(identifier.namespace()).thenReturn(Array("default"))
    when(identifier.name()).thenReturn("table1")

    val table = catalog.loadTable(identifier)
    table should not be null
    table.name() should be("table1")

    val flintTableConf = table.asInstanceOf[FlintReadOnlyTable].conf
    flintTableConf.get("port") should be("9200")
    flintTableConf.get("scheme") should be("http")
    flintTableConf.get("auth") should be("noauth")
    flintTableConf.get("some.other.config") should be("value")
    flintTableConf.containsKey("opensearch.port") should be(false)
    flintTableConf.containsKey("opensearch.scheme") should be(false)
    flintTableConf.containsKey("opensearch.auth") should be(false)
  }

  test("createTable should throw UnsupportedOperationException") {
    val identifier = mock[Identifier]
    val schema = mock[StructType]
    val partitions = Array.empty[Transform]
    val properties = Map.empty[String, String].asJava

    intercept[UnsupportedOperationException] {
      catalog.createTable(identifier, schema, partitions, properties)
    }
  }

  test("alterTable should throw UnsupportedOperationException") {
    val identifier = mock[Identifier]
    val changes = Array.empty[TableChange]

    intercept[UnsupportedOperationException] {
      catalog.alterTable(identifier, changes: _*)
    }
  }

  test("dropTable should throw UnsupportedOperationException") {
    val identifier = mock[Identifier]

    intercept[UnsupportedOperationException] {
      catalog.dropTable(identifier)
    }
  }

  test("renameTable should throw UnsupportedOperationException") {
    val oldIdentifier = mock[Identifier]
    val newIdentifier = mock[Identifier]

    intercept[UnsupportedOperationException] {
      catalog.renameTable(oldIdentifier, newIdentifier)
    }
  }

  test("isDefaultNamespace should return true for default namespace") {
    OpenSearchCatalog.isDefaultNamespace("default") should be(true)
  }

  test("isDefaultNamespace should return false for non-default namespace") {
    OpenSearchCatalog.isDefaultNamespace("non-default") should be(false)
  }

  test("indexNames should split table name by comma") {
    val tableName = "index-1,index-2"
    val result = OpenSearchCatalog.indexNames(tableName)
    result should contain theSameElementsAs Array("index-1", "index-2")
  }
}
