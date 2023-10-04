/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

/**
 * Flint version.
 */
case class FlintVersion(version: String) {

  override def toString: String = version
}

object FlintVersion {
  val V_0_1_0: FlintVersion = FlintVersion("0.1.0")

  def current(): FlintVersion = V_0_1_0
}
