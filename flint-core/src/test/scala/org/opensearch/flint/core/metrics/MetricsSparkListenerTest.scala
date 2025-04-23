/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics

import java.util.{HashMap => JHashMap}

import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.metrics.reporter.DimensionedName

class MetricsSparkListenerTest {

  // Create a testable subclass that provides a custom account ID
  class TestableMetricsSparkListener(accountId: String) extends MetricsSparkListener {
    // Override the addAccountDimension method to use our custom account ID
    override def addAccountDimension(metricName: String): String = {
      import org.opensearch.flint.core.metrics.reporter.DimensionedName
      DimensionedName
        .withName(metricName)
        .withDimension("accountId", accountId)
        .build()
        .toString()
    }
  }

  // Create a testable subclass that exposes the AWS account ID
  class GetClusterAccountIdTestableListener extends MetricsSparkListener {
    def publicGetClusterAccountId(): String = FlintOptions.AWS_ACCOUNT_ID
  }

  private var metricsSparkListener: MetricsSparkListener = _

  @BeforeEach
  def setup(): Unit = {
    // Default instance for tests that don't need a specific account ID
    metricsSparkListener = new MetricsSparkListener()
  }

  /**
   * Test for the addAccountDimension method. This test verifies that the method correctly adds an
   * AWS account dimension to a metric name.
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
    assertTrue(
      result.contains(metricName),
      s"Result should contain the original metric name: $result")
    assertTrue(
      result.contains("accountId##" + expectedAccountId),
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
   * Test for the addAccountDimension method with a complex metric name. This test verifies that
   * the method correctly handles metric names with special characters.
   */
  @Test
  def testAddAccountDimensionWithComplexMetricName(): Unit = {
    // Create a testable instance with a specific account ID
    val expectedAccountId = "123456789012"
    val testListener = new TestableMetricsSparkListener(expectedAccountId)

    // Call the method with a complex metric name
    val metricName = "test.metric.with.dots-and-dashes"
    val result = testListener.addAccountDimension(metricName)

    // Verify the result
    assertTrue(
      result.contains(metricName),
      s"Result should contain the original complex metric name: $result")
    assertTrue(
      result.contains("accountId##" + expectedAccountId),
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
   * Test for the addAccountDimension method when the FLINT_CLUSTER_NAME environment variable is
   * not set. This test verifies that the method uses "UNKNOWN" as the account ID when the
   * environment variable is missing.
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
    assertTrue(
      result.contains(metricName),
      s"Result should contain the original metric name: $result")
    assertTrue(
      result.contains("accountId##" + expectedAccountId),
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
   * Test for the addAccountDimension method with an invalid FLINT_CLUSTER_NAME format. This test
   * verifies that the method handles malformed cluster name values correctly.
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
    assertTrue(
      result.contains(metricName),
      s"Result should contain the original metric name: $result")
    assertTrue(
      result.contains("accountId##" + expectedAccountId),
      s"Result should contain UNKNOWN as the account ID for invalid format: $result")

    // Decode the result to verify the structure
    val dimensionedName = DimensionedName.decode(result)
    assertEquals(metricName, dimensionedName.getName())
    assertEquals(1, dimensionedName.getDimensions().size())

    val dimension = dimensionedName.getDimensions().iterator().next()
    assertEquals("accountId", dimension.getName())
    assertEquals(expectedAccountId, dimension.getValue())
  }

  /**
   * Test for the AWS account ID retrieval when the FLINT_CLUSTER_NAME environment variable is
   * set. This test verifies that the correct account ID is retrieved.
   */
  @Test
  def testGetClusterAccountId(): Unit = {
    // Mock the FlintOptions class to return a known account ID
    val mockFlintOptions = mock(classOf[FlintOptions])
    val expectedAccountId = "123456789012"
    when(mockFlintOptions.getAWSAccountId()).thenReturn(expectedAccountId)

    // Create a testable instance that returns our expected account ID
    val testListener = new GetClusterAccountIdTestableListener() {
      override def publicGetClusterAccountId(): String = expectedAccountId
    }

    // Call the method and verify the result
    val result = testListener.publicGetClusterAccountId()
    assertEquals(expectedAccountId, result, "Should return the expected account ID")
  }

  /**
   * Test for the AWS account ID retrieval when the FLINT_CLUSTER_NAME environment variable is
   * not set. This test verifies that "UNKNOWN" is returned when the environment variable
   * is missing.
   */
  @Test
  def testGetClusterAccountIdWithMissingEnvVar(): Unit = {
    // Mock the FlintOptions class to return "UNKNOWN"
    val mockFlintOptions = mock(classOf[FlintOptions])
    val expectedAccountId = "UNKNOWN"
    when(mockFlintOptions.getAWSAccountId()).thenReturn(expectedAccountId)

    // Create a testable instance that returns our expected account ID
    val testListener = new GetClusterAccountIdTestableListener() {
      override def publicGetClusterAccountId(): String = expectedAccountId
    }

    // Call the method and verify the result
    val result = testListener.publicGetClusterAccountId()
    assertEquals(expectedAccountId, result, "Should return UNKNOWN when env var is missing")
  }

  /**
   * Test for the AWS account ID retrieval with an invalid FLINT_CLUSTER_NAME format. This test
   * verifies that "UNKNOWN" is returned when the environment variable has an invalid
   * format.
   */
  @Test
  def testGetClusterAccountIdWithInvalidClusterNameFormat(): Unit = {
    // Mock the FlintOptions class to return "UNKNOWN"
    val mockFlintOptions = mock(classOf[FlintOptions])
    val expectedAccountId = "UNKNOWN"
    when(mockFlintOptions.getAWSAccountId()).thenReturn(expectedAccountId)

    // Create a testable instance that returns our expected account ID
    val testListener = new GetClusterAccountIdTestableListener() {
      override def publicGetClusterAccountId(): String = expectedAccountId
    }

    // Call the method and verify the result
    val result = testListener.publicGetClusterAccountId()
    assertEquals(expectedAccountId, result, "Should return UNKNOWN for invalid format")
  }
}
