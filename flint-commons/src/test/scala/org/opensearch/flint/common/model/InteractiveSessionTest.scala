/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.common.model

import java.util.{HashMap => JavaHashMap}

import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite

class InteractiveSessionTest extends SparkFunSuite with Matchers {

  test("deserialize should correctly parse a FlintInstance with excludedJobIds from JSON") {
    val json =
      """{"applicationId":"app-123","jobId":"job-456","sessionId":"session-789","state":"RUNNING","lastUpdateTime":1620000000000,"jobStartTime":1620000001000,"excludeJobIds":["job-101","job-202"]}"""
    val instance = InteractiveSession.deserialize(json)

    instance.applicationId shouldBe "app-123"
    instance.jobId shouldBe "job-456"
    instance.sessionId shouldBe "session-789"
    instance.state shouldBe "RUNNING"
    instance.lastUpdateTime shouldBe 1620000000000L
    instance.jobStartTime shouldBe 1620000001000L
    instance.excludedJobIds should contain allOf ("job-101", "job-202")
    instance.error shouldBe None
  }

  test("serialize should correctly produce JSON from a FlintInstance with excludedJobIds") {
    val excludedJobIds = Seq("job-101", "job-202")
    val instance = new InteractiveSession(
      "app-123",
      "job-456",
      "session-789",
      "RUNNING",
      1620000000000L,
      1620000001000L,
      excludedJobIds)
    val currentTime = System.currentTimeMillis()
    val json = InteractiveSession.serializeWithoutJobId(instance, currentTime)

    json should include(""""applicationId":"app-123"""")
    json should not include (""""jobId":"job-456"""")
    json should include(""""sessionId":"session-789"""")
    json should include(""""state":"RUNNING"""")
    json should include(s""""lastUpdateTime":$currentTime""")
    json should include(
      """"excludeJobIds":"job-101,job-202""""
    ) // Check for comma-separated string
    json should include(""""jobStartTime":1620000001000""")
    json should include(""""error":""""")
  }

  test("deserialize should correctly handle an empty excludedJobIds field in JSON") {
    val jsonWithoutExcludedJobIds =
      """{"applicationId":"app-123","jobId":"job-456","sessionId":"session-789","state":"RUNNING","lastUpdateTime":1620000000000,"jobStartTime":1620000001000}"""
    val instance = InteractiveSession.deserialize(jsonWithoutExcludedJobIds)

    instance.excludedJobIds shouldBe empty
  }

  test("deserialize should correctly handle error field in JSON") {
    val jsonWithError =
      """{"applicationId":"app-123","jobId":"job-456","sessionId":"session-789","state":"FAILED","lastUpdateTime":1620000000000,"jobStartTime":1620000001000,"error":"Some error occurred"}"""
    val instance = InteractiveSession.deserialize(jsonWithError)

    instance.error shouldBe Some("Some error occurred")
  }

  test("serialize should include error when present in FlintInstance") {
    val instance = new InteractiveSession(
      "app-123",
      "job-456",
      "session-789",
      "FAILED",
      1620000000000L,
      1620000001000L,
      Seq.empty[String],
      Some("Some error occurred"))
    val currentTime = System.currentTimeMillis()
    val json = InteractiveSession.serializeWithoutJobId(instance, currentTime)

    json should include(""""error":"Some error occurred"""")
  }

  test("deserializeFromMap should handle normal case") {
    val sourceMap = new JavaHashMap[String, AnyRef]()
    sourceMap.put("applicationId", "app1")
    sourceMap.put("jobId", "job1")
    sourceMap.put("sessionId", "session1")
    sourceMap.put("state", "running")
    sourceMap.put("lastUpdateTime", java.lang.Long.valueOf(1234567890L))
    sourceMap.put("jobStartTime", java.lang.Long.valueOf(9876543210L))
    sourceMap.put("excludeJobIds", java.util.Arrays.asList("job2", "job3"))
    sourceMap.put("error", "An error occurred")

    val result = InteractiveSession.deserializeFromMap(sourceMap)

    assert(result.applicationId == "app1")
    assert(result.jobId == "job1")
    assert(result.sessionId == "session1")
    assert(result.state == "running")
    assert(result.lastUpdateTime == 1234567890L)
    assert(result.jobStartTime == 9876543210L)
    assert(result.excludedJobIds == Seq("job2", "job3"))
    assert(result.error.contains("An error occurred"))
  }

  test("deserializeFromMap should handle incorrect field types") {
    val sourceMap = new JavaHashMap[String, AnyRef]()
    sourceMap.put("applicationId", Integer.valueOf(123))
    sourceMap.put("lastUpdateTime", "1234567890")

    assertThrows[ClassCastException] {
      InteractiveSession.deserializeFromMap(sourceMap)
    }
  }

  test("deserializeFromMap should handle missing jobStartTime") {
    val sourceMap = new JavaHashMap[String, AnyRef]()
    sourceMap.put("applicationId", "app1")
    sourceMap.put("jobId", "job1")
    sourceMap.put("sessionId", "session1")
    sourceMap.put("state", "running")
    sourceMap.put("lastUpdateTime", java.lang.Long.valueOf(1234567890L))
    // jobStartTime is not added, simulating its absence
    sourceMap.put("excludeJobIds", java.util.Arrays.asList("job2", "job3"))
    sourceMap.put("error", "An error occurred")

    val result = InteractiveSession.deserializeFromMap(sourceMap)

    assert(result.applicationId == "app1")
    assert(result.jobId == "job1")
    assert(result.sessionId == "session1")
    assert(result.state == "running")
    assert(result.lastUpdateTime == 1234567890L)
    assert(result.jobStartTime == 0L) // Default value for missing jobStartTime
    assert(result.excludedJobIds == Seq("job2", "job3"))
    assert(result.error.contains("An error occurred"))
  }

  test("deserialize should correctly parse a FlintInstance without jobStartTime from JSON") {
    val json =
      """{"applicationId":"app-123","jobId":"job-456","sessionId":"session-789","state":"RUNNING","lastUpdateTime":1620000000000,"excludeJobIds":["job-101","job-202"]}"""
    val instance = InteractiveSession.deserialize(json)

    instance.applicationId shouldBe "app-123"
    instance.jobId shouldBe "job-456"
    instance.sessionId shouldBe "session-789"
    instance.state shouldBe "RUNNING"
    instance.lastUpdateTime shouldBe 1620000000000L
    instance.jobStartTime shouldBe 0L // Default or expected value for missing jobStartTime
    instance.excludedJobIds should contain allOf ("job-101", "job-202")
    instance.error shouldBe None
  }

  test("sessionContext should add/get key-value pairs to/from the context") {
    val session =
      new InteractiveSession("app-123", "job-456", "session-789", "RUNNING", 1620000000000L)
    session.context shouldBe empty

    session.setContextValue("key1", "value1")
    session.setContextValue("key2", 42)

    session.context should contain("key1" -> "value1")
    session.context should contain("key2" -> 42)

    session.getContextValue("key1") shouldBe Some("value1")
    session.getContextValue("key2") shouldBe Some(42)
    session.getContextValue("key3") shouldBe None // Test for a key that does not exist

    session.setContextValue("key1", "updatedValue")
    session.getContextValue("key1") shouldBe Some("updatedValue")
  }
}
