/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.datatype

import org.apache.spark.sql.types.{MetadataBuilder, _}

/**
 * Helper class for handling Flint metadata operations
 */
object FlintMetadataExtensions {
  // OpenSearch Mappings. https://opensearch.org/docs/latest/field-types/
  val OS_TYPE_KEY = "osType"
  val FIELDS_NAMES_KEY = "fields"

  // OpenSearch field types. https://opensearch.org/docs/latest/field-types/supported-field-types/index/
  val TEXT_TYPE = "text"
  val KEYWORD_TYPE = "keyword"
  val HALF_FLOAT_TYPE = "half_float"

  implicit class MetadataExtension(val metadata: Metadata) {

    /**
     * Determines if this metadata represents a text field.
     *
     * @return
     *   `true` if the metadata contains an `osType` property set to "text", `false` otherwise
     */
    def isTextField: Boolean =
      metadata.contains(OS_TYPE_KEY) && metadata.getString(OS_TYPE_KEY) == TEXT_TYPE

    /**
     * Retrieves the first keyword subfield from multi-field definitions.
     *
     * @return
     *   [[Some]] containing the first keyword subfield name if defined, [[None]] if no keyword
     *   subfields exist
     */
    def keywordSubfield: Option[String] = {
      if (metadata.contains(FIELDS_NAMES_KEY)) {
        val multiFieldMetadata = metadata.getMetadata(FIELDS_NAMES_KEY)
        if (multiFieldMetadata.contains(KEYWORD_TYPE)) {
          multiFieldMetadata
            .getStringArray(KEYWORD_TYPE)
            .headOption
        } else {
          None
        }
      } else {
        None
      }
    }

    def isHalfFloatField: Boolean =
      metadata.contains(OS_TYPE_KEY) && metadata.getString(OS_TYPE_KEY) == HALF_FLOAT_TYPE
  }

  implicit class MetadataBuilderExtension(val builder: MetadataBuilder) {

    /**
     * Marks the field as a text field in OpenSearch metadata.
     *
     * @return
     *   the modified [[MetadataBuilder]] instance for method chaining
     * @see
     *   [[https://opensearch.org/docs/latest/field-types/supported-field-types/text/ Text Field Documentation]]
     */
    def withTextField(): MetadataBuilder = {
      builder.putString(OS_TYPE_KEY, TEXT_TYPE)
      builder
    }

    /**
     * Adds multi-field definitions to the metadata.
     *
     * @param fields
     *   Map where keys are field names and values are OpenSearch field types
     * @return
     *   the modified [[MetadataBuilder]] instance for method chaining
     * @example
     *   {{{builder.withMultiFields(Map( "raw" -> KEYWORD_TYPE, "analyzed" -> TEXT_TYPE ))}}}
     * @note
     *   Groups fields by type and stores them under the `fields` metadata key
     */
    def withMultiFields(fields: Map[String, String]): MetadataBuilder = {
      val nestedBuilder = new MetadataBuilder()
      fields
        .groupBy { case (_, fieldType) => fieldType }
        .foreach { case (fieldType, entries) =>
          nestedBuilder.putStringArray(fieldType, entries.keys.toArray)
        }
      builder.putMetadata(FIELDS_NAMES_KEY, nestedBuilder.build())
      builder
    }

    /**
     * Marks the field as a half_float field in OpenSearch metadata.
     *
     * @return
     *   the modified [[MetadataBuilder]] instance for method chaining
     */
    def withHalfFloat(): MetadataBuilder = {
      builder.putString(OS_TYPE_KEY, HALF_FLOAT_TYPE)
      builder
    }
  }
}
