/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import scala.collection.JavaConverters._

import org.opensearch.flint.OpenSearchSuite
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FlintClientBuilderSuite extends AnyFlatSpec with OpenSearchSuite with Matchers {

  behavior of "Flint client builder"

  it should "build flint client" in {
    val flintClient = FlintClientBuilder.build(new FlintOptions(openSearchOptions.asJava))
    flintClient shouldBe a[FlintClient]
  }

  it should "fail if cannot instantiate metadata log service" in {
    val options =
      openSearchOptions + (FlintOptions.CUSTOM_FLINT_METADATA_LOG_SERVICE_CLASS -> "org.dummy.Class")
    the[RuntimeException] thrownBy {
      FlintClientBuilder.build(new FlintOptions(options.asJava))
    }
  }
}
