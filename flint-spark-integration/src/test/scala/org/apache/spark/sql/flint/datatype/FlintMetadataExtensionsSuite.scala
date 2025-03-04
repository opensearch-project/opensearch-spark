/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.datatype

import org.scalatest.matchers.should.Matchers

import org.apache.spark.FlintSuite
import org.apache.spark.sql.flint.datatype.FlintMetadataExtensions.{MetadataBuilderExtension, MetadataExtension}
import org.apache.spark.sql.types._

class FlintMetadataExtensionsSuite extends FlintSuite with Matchers {

  test("isTextField returns true when osType is text") {
    val builder = new MetadataBuilder()
      .putString(FlintMetadataExtensions.OS_TYPE_KEY, FlintMetadataExtensions.TEXT_TYPE)
    val metadata: Metadata = builder.build()
    assert(metadata.isTextField)
  }

  test("isTextField returns false when osType is not text") {
    val builder = new MetadataBuilder().putString(FlintMetadataExtensions.OS_TYPE_KEY, "non-text")
    val metadata: Metadata = builder.build()
    assert(!metadata.isTextField)
  }

  test("addTextFieldMetadata sets osType to text") {
    val builder = new MetadataBuilder()
    val updatedBuilder = builder.withTextField
    val metadata: Metadata = updatedBuilder.build()
    assert(
      metadata.getString(
        FlintMetadataExtensions.OS_TYPE_KEY) == FlintMetadataExtensions.TEXT_TYPE)
  }

  test("addMultiFieldMetadata groups fields by field type") {
    val builder = new MetadataBuilder()
    val fields = Map(
      "field1" -> FlintMetadataExtensions.TEXT_TYPE,
      "field2" -> FlintMetadataExtensions.KEYWORD_TYPE,
      "field3" -> FlintMetadataExtensions.KEYWORD_TYPE)
    val updatedBuilder = builder.withMultiFields(fields)
    val metadata: Metadata = updatedBuilder.build()

    // Verify that multi-field metadata is added under FIELDS_NAMES_KEY.
    assert(metadata.contains(FlintMetadataExtensions.FIELDS_NAMES_KEY))
    val multiFieldMetadata: Metadata =
      metadata.getMetadata(FlintMetadataExtensions.FIELDS_NAMES_KEY)

    // Verify text type field grouping.
    assert(multiFieldMetadata.contains(FlintMetadataExtensions.TEXT_TYPE))
    val textFields = multiFieldMetadata.getStringArray(FlintMetadataExtensions.TEXT_TYPE)
    assert(textFields.sameElements(Array("field1")))

    // Verify keyword type field grouping.
    assert(multiFieldMetadata.contains(FlintMetadataExtensions.KEYWORD_TYPE))
    val keywordFields = multiFieldMetadata.getStringArray(FlintMetadataExtensions.KEYWORD_TYPE)
    // Since the order of grouping may vary, compare sorted arrays.
    assert(keywordFields.sorted.sameElements(Array("field2", "field3")))
  }

  test("getKeywordSubfield returns the first keyword field if available") {
    val builder = new MetadataBuilder()
    val fields = Map(
      "field1" -> FlintMetadataExtensions.TEXT_TYPE,
      "field2" -> FlintMetadataExtensions.KEYWORD_TYPE,
      "field3" -> FlintMetadataExtensions.KEYWORD_TYPE)
    val updatedBuilder = builder.withMultiFields(fields)
    val metadata: Metadata = updatedBuilder.build()

    // Retrieve keyword fields from the nested metadata.
    val multiFieldMetadata = metadata.getMetadata(FlintMetadataExtensions.FIELDS_NAMES_KEY)
    val keywordFields = multiFieldMetadata.getStringArray(FlintMetadataExtensions.KEYWORD_TYPE)

    // Expect the first keyword field.
    assert(metadata.keywordSubfield == keywordFields.headOption)
  }

  test("getKeywordSubfield returns None if no keyword field exists") {
    val builder = new MetadataBuilder()
    val fields = Map("field1" -> FlintMetadataExtensions.TEXT_TYPE)
    val updatedBuilder = builder.withMultiFields(fields)
    val metadata: Metadata = updatedBuilder.build()

    // Since there is no keyword type, getKeywordSubfield should return None.
    assert(metadata.keywordSubfield.isEmpty)
  }
}
