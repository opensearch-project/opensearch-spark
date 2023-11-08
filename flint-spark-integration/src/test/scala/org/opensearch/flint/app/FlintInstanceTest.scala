/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.app

import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite

class FlintInstanceTest extends SparkFunSuite with Matchers {

  test("deserialize should correctly parse a FlintInstance from JSON") {
    val json =
      """{"applicationId":"app-123","jobId":"job-456","sessionId":"session-789","state":"RUNNING","lastUpdateTime":1620000000000,"jobStartTime":1620000001000}"""
    val instance = FlintInstance.deserialize(json)

    instance.applicationId shouldBe "app-123"
    instance.jobId shouldBe "job-456"
    instance.sessionId shouldBe "session-789"
    instance.state shouldBe "RUNNING"
    instance.lastUpdateTime shouldBe 1620000000000L
    instance.jobStartTime shouldBe 1620000001000L
    instance.error shouldBe None
  }

  test("serialize should correctly produce JSON from a FlintInstance") {
    val instance = new FlintInstance(
      "app-123",
      "job-456",
      "session-789",
      "RUNNING",
      1620000000000L,
      1620000001000L)
    val currentTime = System.currentTimeMillis()
    val json = FlintInstance.serialize(instance, currentTime)

    json should include(""""applicationId":"app-123"""")
    json should not include(""""jobId":"job-456"""")
    json should include(""""sessionId":"session-789"""")
    json should include(""""state":"RUNNING"""")
    json should include(s""""lastUpdateTime":$currentTime""")
    json should include(""""jobStartTime":1620000001000""")
    json should include(""""error":""""")
  }

  test("deserialize should correctly handle error field in JSON") {
    val jsonWithError =
      """{"applicationId":"app-123","jobId":"job-456","sessionId":"session-789","state":"FAILED","lastUpdateTime":1620000000000,"jobStartTime":1620000001000,"error":"Some error occurred"}"""
    val instance = FlintInstance.deserialize(jsonWithError)

    instance.error shouldBe Some("Some error occurred")
  }

  test("serialize should include error when present in FlintInstance") {
    val instance = new FlintInstance(
      "app-123",
      "job-456",
      "session-789",
      "FAILED",
      1620000000000L,
      1620000001000L,
      Some("Some error occurred"))
    val currentTime = System.currentTimeMillis()
    val json = FlintInstance.serialize(instance, currentTime)

    json should include(""""error":"Some error occurred"""")
  }
}
