/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package shaded.org.apache.http;

import java.io.IOException;

/**
 * Test-only stub that mimics the SHADED Apache HTTP {@code ConnectionClosedException} seen at
 * runtime (a relocated {@code org.apache.http.ConnectionClosedException}). It deliberately
 * lives in a different package from {@code org.apache.http} but keeps the same simple class name,
 * so tests can verify that {@code ExceptionClassNameFailurePredicate} matches by simple name and is
 * therefore independent of shading/relocation.
 */
public class ConnectionClosedException extends IOException {
  public ConnectionClosedException(String message) {
    super(message);
  }
}
