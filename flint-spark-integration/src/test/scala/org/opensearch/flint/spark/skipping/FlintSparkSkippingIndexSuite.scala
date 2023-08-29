/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.mockito.Mockito.when
import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.FILE_PATH_COLUMN
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet
import org.apache.spark.sql.functions.col

class FlintSparkSkippingIndexSuite extends FlintSuite {

  test("get skipping index name") {
    val index = new FlintSparkSkippingIndex("default.test", Seq(mock[FlintSparkSkippingStrategy]))
    index.name() shouldBe "flint_default_test_skipping_index"
  }

  test("can build index building job with unique ID column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(Map("name" -> "string"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("name").expr)))
    val index = new FlintSparkSkippingIndex("default.test", Seq(indexCol))

    val df = spark.createDataFrame(Seq(("hello", 20))).toDF("name", "age")
    val indexDf = index.build(df)
    indexDf.schema.fieldNames should contain only ("name", FILE_PATH_COLUMN, ID_COLUMN)
  }

  test("can build index for boolean column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(Map("boolean_col" -> "boolean"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("boolean_col").expr)))

    val index = new FlintSparkSkippingIndex("default.test", Seq(indexCol))
    index.metadata().getContent should matchJson(
      s"""{
         |   "_meta": {
         |     "name": "flint_default_test_skipping_index",
         |     "kind": "skipping",
         |     "indexedColumns": [{}],
         |     "source": "default.test"
         |   },
         |   "properties": {
         |     "boolean_col": {
         |       "type": "boolean"
         |     },
         |     "file_path": {
         |       "type": "keyword"
         |     }
         |   }
         | }
         |""".stripMargin)
  }

  test("can build index for string column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(Map("string_col" -> "string"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("string_col").expr)))

    val index = new FlintSparkSkippingIndex("default.test", Seq(indexCol))
    index.metadata().getContent should matchJson(
      s"""{
         |   "_meta": {
         |     "name": "flint_default_test_skipping_index",
         |     "kind": "skipping",
         |     "indexedColumns": [{}],
         |     "source": "default.test"
         |   },
         |   "properties": {
         |     "string_col": {
         |       "type": "keyword"
         |     },
         |     "file_path": {
         |       "type": "keyword"
         |     }
         |   }
         | }
         |""".stripMargin)
  }

  // TODO: test for osType "text"

  test("can build index for long column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(Map("long_col" -> "bigint"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("long_col").expr)))

    val index = new FlintSparkSkippingIndex("default.test", Seq(indexCol))
    index.metadata().getContent should matchJson(
      s"""{
         |   "_meta": {
         |     "name": "flint_default_test_skipping_index",
         |     "kind": "skipping",
         |     "indexedColumns": [{}],
         |     "source": "default.test"
         |   },
         |   "properties": {
         |     "long_col": {
         |       "type": "long"
         |     },
         |     "file_path": {
         |       "type": "keyword"
         |     }
         |   }
         | }
         |""".stripMargin)
  }

  test("can build index for int column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(Map("int_col" -> "int"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("int_col").expr)))

    val index = new FlintSparkSkippingIndex("default.test", Seq(indexCol))
    index.metadata().getContent should matchJson(
      s"""{
         |   "_meta": {
         |     "name": "flint_default_test_skipping_index",
         |     "kind": "skipping",
         |     "indexedColumns": [{}],
         |     "source": "default.test"
         |   },
         |   "properties": {
         |     "int_col": {
         |       "type": "integer"
         |     },
         |     "file_path": {
         |       "type": "keyword"
         |     }
         |   }
         | }
         |""".stripMargin)
  }

  test("can build index for short column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(Map("short_col" -> "smallint"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("short_col").expr)))

    val index = new FlintSparkSkippingIndex("default.test", Seq(indexCol))
    index.metadata().getContent should matchJson(
      s"""{
         |   "_meta": {
         |     "name": "flint_default_test_skipping_index",
         |     "kind": "skipping",
         |     "indexedColumns": [{}],
         |     "source": "default.test"
         |   },
         |   "properties": {
         |     "short_col": {
         |       "type": "short"
         |     },
         |     "file_path": {
         |       "type": "keyword"
         |     }
         |   }
         | }
         |""".stripMargin)
  }

  test("can build index for byte column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(Map("byte_col" -> "tinyint"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("byte_col").expr)))

    val index = new FlintSparkSkippingIndex("default.test", Seq(indexCol))
    index.metadata().getContent should matchJson(
      s"""{
         |   "_meta": {
         |     "name": "flint_default_test_skipping_index",
         |     "kind": "skipping",
         |     "indexedColumns": [{}],
         |     "source": "default.test"
         |   },
         |   "properties": {
         |     "byte_col": {
         |       "type": "byte"
         |     },
         |     "file_path": {
         |       "type": "keyword"
         |     }
         |   }
         | }
         |""".stripMargin)
  }

  test("can build index for double column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(Map("double_col" -> "double"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("double_col").expr)))

    val index = new FlintSparkSkippingIndex("default.test", Seq(indexCol))
    index.metadata().getContent should matchJson(
      s"""{
         |   "_meta": {
         |     "name": "flint_default_test_skipping_index",
         |     "kind": "skipping",
         |     "indexedColumns": [{}],
         |     "source": "default.test"
         |   },
         |   "properties": {
         |     "double_col": {
         |       "type": "double"
         |     },
         |     "file_path": {
         |       "type": "keyword"
         |     }
         |   }
         | }
         |""".stripMargin)
  }

  test("can build index for float column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(Map("float_col" -> "float"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("float_col").expr)))

    val index = new FlintSparkSkippingIndex("default.test", Seq(indexCol))
    index.metadata().getContent should matchJson(
      s"""{
         |   "_meta": {
         |     "name": "flint_default_test_skipping_index",
         |     "kind": "skipping",
         |     "indexedColumns": [{}],
         |     "source": "default.test"
         |   },
         |   "properties": {
         |     "float_col": {
         |       "type": "float"
         |     },
         |     "file_path": {
         |       "type": "keyword"
         |     }
         |   }
         | }
         |""".stripMargin)
  }

  test("can build index for timestamp column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(Map("timestamp_col" -> "timestamp"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("timestamp_col").expr)))

    val index = new FlintSparkSkippingIndex("default.test", Seq(indexCol))
    index.metadata().getContent should matchJson(
      s"""{
         |   "_meta": {
         |     "name": "flint_default_test_skipping_index",
         |     "kind": "skipping",
         |     "indexedColumns": [{}],
         |     "source": "default.test"
         |   },
         |   "properties": {
         |     "timestamp_col": {
         |       "type": "date",
         |       "format": "strict_date_optional_time_nanos"
         |     },
         |     "file_path": {
         |       "type": "keyword"
         |     }
         |   }
         | }
         |""".stripMargin)
  }

  test("can build index for date column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(Map("date_col" -> "date"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("date_col").expr)))

    val index = new FlintSparkSkippingIndex("default.test", Seq(indexCol))
    index.metadata().getContent should matchJson(
      s"""{
         |   "_meta": {
         |     "name": "flint_default_test_skipping_index",
         |     "kind": "skipping",
         |     "indexedColumns": [{}],
         |     "source": "default.test"
         |   },
         |   "properties": {
         |     "date_col": {
         |       "type": "date",
         |       "format": "strict_date"
         |     },
         |     "file_path": {
         |       "type": "keyword"
         |     }
         |   }
         | }
         |""".stripMargin)
  }

  test("can build index for struct column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(
      Map("struct_col" -> "struct<subfield1:string,subfield2:int>"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("struct_col").expr)))

    val index = new FlintSparkSkippingIndex("default.test", Seq(indexCol))
    index.metadata().getContent should matchJson(
      s"""{
         |   "_meta": {
         |     "name": "flint_default_test_skipping_index",
         |     "kind": "skipping",
         |     "indexedColumns": [{}],
         |     "source": "default.test"
         |   },
         |   "properties": {
         |     "struct_col": {
         |       "properties": {
         |         "subfield1": {
         |           "type": "keyword"
         |         },
         |         "subfield2": {
         |           "type": "integer"
         |         }
         |       }
         |     },
         |     "file_path": {
         |       "type": "keyword"
         |     }
         |   }
         | }
         |""".stripMargin)
  }

  test("should fail if get index name without full table name") {
    assertThrows[IllegalArgumentException] {
      FlintSparkSkippingIndex.getSkippingIndexName("test")
    }
  }

  test("should fail if no indexed column given") {
    assertThrows[IllegalArgumentException] {
      new FlintSparkSkippingIndex("default.test", Seq.empty)
    }
  }
}
