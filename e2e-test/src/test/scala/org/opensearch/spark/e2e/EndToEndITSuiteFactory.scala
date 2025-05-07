/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.spark.e2e

import org.scalatest.Suite

/**
 * Factory for creating the appropriate EndToEndITSuite implementation
 * based on the environment.
 */
object EndToEndITSuiteFactory {
  /**
   * Creates an EndToEndITSuite implementation appropriate for the current environment.
   * In GitHub Actions, it returns a suite with mock S3 client.
   * In local environment, it returns the standard suite.
   *
   * @return EndToEndITSuite implementation
   */
  def createSuite(): Suite = {
    val isGitHubActions = sys.env.getOrElse("GITHUB_ACTIONS", "false").toLowerCase == "true"
    
    if (isGitHubActions) {
      new GitHubActionsEndToEndITSuite
    } else {
      new StandardEndToEndITSuite
    }
  }
  
  /**
   * EndToEndITSuite implementation for GitHub Actions environment.
   * Uses mock S3 client to avoid connecting to a real S3 service.
   */
  class GitHubActionsEndToEndITSuite extends EndToEndITSuite with GitHubActionsS3ClientTrait
  
  /**
   * Standard EndToEndITSuite implementation for local environment.
   */
  class StandardEndToEndITSuite extends EndToEndITSuite
}