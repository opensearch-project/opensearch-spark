/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.datatype

import org.apache.spark.sql.types.{MetadataBuilder, _}

/**
 * Helper class for handling Flint metadata operations
 */
object FlintMetadataHelper {
  // OpenSearch Mappings. https://opensearch.org/docs/latest/field-types/
  val OS_TYPE_KEY = "osType"
  val FIELDS_NAMES_KEY = "fields"

  // OpenSearch field types. https://opensearch.org/docs/latest/field-types/supported-field-types/index/
  val TEXT_TYPE = "text"
  val KEYWORD_TYPE = "keyword"

  /**
   * Check if the metadata indicates a text field
   */
  def isTextField(metadata: Metadata): Boolean = {
    metadata.contains(OS_TYPE_KEY) && metadata.getString(OS_TYPE_KEY) == TEXT_TYPE
  }

  /**
   * Add text field metadata to builder
   */
  def addTextFieldMetadata(builder: MetadataBuilder): MetadataBuilder = {
    builder.putString(OS_TYPE_KEY, TEXT_TYPE)
  }

  /**
   * Add multi-field metadata to the provided MetadataBuilder.
   *
   * This method groups the provided fields by their field type. For each field type, the
   * associated field names are collected into an array. These arrays are then stored in a nested
   * metadata object, with each field type as the key. The nested metadata is added to the main
   * metadata builder under the key FIELDS_NAMES_KEY.
   *
   * @param builder
   *   the MetadataBuilder to update with multi-field metadata.
   * @param fields
   *   a map where each key is a field name and the corresponding value is its field type.
   * @return
   *   the updated MetadataBuilder containing the multi-field metadata.
   */
  def addMultiFieldMetadata(
      builder: MetadataBuilder,
      fields: Map[String, String]): MetadataBuilder = {
    val mb = new MetadataBuilder()
    fields
      .groupBy { case (_, fieldType) => fieldType }
      .foreach { case (fieldType, entries) =>
        val fieldNames = entries.map { case (fieldName, _) => fieldName }
        mb.putStringArray(fieldType, fieldNames.toArray)
      }
    builder.putMetadata(FIELDS_NAMES_KEY, mb.build())
  }

  /**
   * Retrieve the first subfield name of type KEYWORD_TYPE if available.
   *
   * This method checks whether the provided metadata contains multi-field metadata under the key
   * FIELDS_NAMES_KEY. It then looks for a group of subfields with the key equal to KEYWORD_TYPE.
   * If such a group exists, the first field name in the array is returned.
   *
   * @param metadata
   *   the metadata from which to retrieve the keyword subfield.
   * @return
   *   an Option containing the first keyword subfield name, if found; otherwise, None.
   */
  def getKeywordSubfield(metadata: Metadata): Option[String] = {
    if (metadata.contains(FIELDS_NAMES_KEY)) {
      val multiFieldMetadata = metadata.getMetadata(FIELDS_NAMES_KEY)
      if (multiFieldMetadata.contains(KEYWORD_TYPE)) {
        multiFieldMetadata.getStringArray(KEYWORD_TYPE).headOption
      } else {
        None
      }
    } else {
      None
    }
  }
}
