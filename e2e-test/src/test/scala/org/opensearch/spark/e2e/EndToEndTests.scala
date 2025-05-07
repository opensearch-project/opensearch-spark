/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.spark.e2e

import org.scalatest.Suite

/**
 * Entry point for running the end-to-end tests.
 * Uses the factory to create the appropriate test suite based on the environment.
 */
class EndToEndTests extends Suite {
  // Create the appropriate test suite using the factory
  val suite = EndToEndITSuiteFactory.createSuite()
  
  // Delegate all test methods to the created suite
  override def run(testName: Option[String], args: org.scalatest.Args): org.scalatest.Status = {
    suite.run(testName, args)
  }
}