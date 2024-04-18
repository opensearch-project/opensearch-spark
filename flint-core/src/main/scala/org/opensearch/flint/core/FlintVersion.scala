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
  val V_0_2_0: FlintVersion = FlintVersion("0.2.0")
  val V_0_3_0: FlintVersion = FlintVersion("0.3.0")
  val V_0_4_0: FlintVersion = FlintVersion("0.4.0")

  def current(): FlintVersion = V_0_4_0
}
