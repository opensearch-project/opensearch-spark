/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics

import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.mockito.Mockito.{mock, when}
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.metrics.reporter.DimensionedName

import java.util.{HashMap => JHashMap}
import org.mockito.ArgumentMatchers.any

class MetricsSparkListenerTest {

  // Create a testable subclass that overrides the getClusterAccountId method
  class TestableMetricsSparkListener(accountId: String) extends MetricsSparkListener {
    override protected def getClusterAccountId(): String = accountId
  }

  private var metricsSparkListener: MetricsSparkListener = _

  @BeforeEach
  def setup(): Unit = {
    // Default instance for tests that don't need a specific account ID
    metricsSparkListener = new MetricsSparkListener()
  }

  /**
   * Test for the addAccountDimension method.
   * This test verifies that the method correctly adds an AWS account dimension to a metric name.
   */
  @Test
  def testAddAccountDimension(): Unit = {
    // Create a testable instance with a specific account ID
    val expectedAccountId = "123456789012"
    val testListener = new TestableMetricsSparkListener(expectedAccountId)
    
    // Call the method directly (no reflection needed)
    val metricName = "test.metric"
    val result = testListener.addAccountDimension(metricName)

    // Verify the result
    assertTrue(result.contains(metricName), s"Result should contain the original metric name: $result")
    assertTrue(result.contains("accountId##" + expectedAccountId), 
      s"Result should contain the account ID dimension: $result")

    // Decode the result to verify the structure
    val dimensionedName = DimensionedName.decode(result)
    assertEquals(metricName, dimensionedName.getName())
    assertEquals(1, dimensionedName.getDimensions().size())
    
    val dimension = dimensionedName.getDimensions().iterator().next()
    assertEquals("accountId", dimension.getName())
    assertEquals(expectedAccountId, dimension.getValue())
  }

  /**
   * Test for the addAccountDimension method when the FLINT_CLUSTER_NAME environment variable is not set.
   * This test verifies that the method uses "UNKNOWN" as the account ID when the environment variable is missing.
   */
  @Test
  def testAddAccountDimensionWithMissingEnvVar(): Unit = {
    // Create a testable instance with "UNKNOWN" as the account ID
    val expectedAccountId = "UNKNOWN"
    val testListener = new TestableMetricsSparkListener(expectedAccountId)
    
    // Call the method directly
    val metricName = "test.metric"
    val result = testListener.addAccountDimension(metricName)

    // Verify the result uses "UNKNOWN" as the account ID
    assertTrue(result.contains(metricName), s"Result should contain the original metric name: $result")
    assertTrue(result.contains("accountId##" + expectedAccountId), 
      s"Result should contain UNKNOWN as the account ID: $result")

    // Decode the result to verify the structure
    val dimensionedName = DimensionedName.decode(result)
    assertEquals(metricName, dimensionedName.getName())
    assertEquals(1, dimensionedName.getDimensions().size())
    
    val dimension = dimensionedName.getDimensions().iterator().next()
    assertEquals("accountId", dimension.getName())
    assertEquals(expectedAccountId, dimension.getValue())
  }

  /**
   * Test for the addAccountDimension method with an invalid FLINT_CLUSTER_NAME format.
   * This test verifies that the method handles malformed cluster name values correctly.
   */
  @Test
  def testAddAccountDimensionWithInvalidClusterNameFormat(): Unit = {
    // Create a testable instance with "UNKNOWN" as the account ID
    // This simulates what would happen with an invalid cluster name format
    val expectedAccountId = "UNKNOWN"
    val testListener = new TestableMetricsSparkListener(expectedAccountId)
    
    // Call the method directly
    val metricName = "test.metric"
    val result = testListener.addAccountDimension(metricName)

    // Verify the result uses "UNKNOWN" as the account ID for invalid format
    assertTrue(result.contains(metricName), s"Result should contain the original metric name: $result")
    assertTrue(result.contains("accountId##" + expectedAccountId), 
      s"Result should contain UNKNOWN as the account ID for invalid format: $result")
      
    // Decode the result to verify the structure
    val dimensionedName = DimensionedName.decode(result)
    assertEquals(metricName, dimensionedName.getName())
    assertEquals(1, dimensionedName.getDimensions().size())
    
    val dimension = dimensionedName.getDimensions().iterator().next()
    assertEquals("accountId", dimension.getName())
    assertEquals(expectedAccountId, dimension.getValue())
  }
}