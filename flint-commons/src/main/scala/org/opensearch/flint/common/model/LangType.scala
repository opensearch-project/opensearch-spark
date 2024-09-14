/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.common.scheduler.model

object LangType {
  val SQL = "sql"
  val PPL = "ppl"

  private val values = Seq(SQL, PPL)

  /**
   * Get LangType from text.
   *
   * @param text
   *   input text
   * @return
   *   Option[String] if found, None otherwise
   */
  def fromString(text: String): Option[String] = {
    values.find(_.equalsIgnoreCase(text))
  }
}
