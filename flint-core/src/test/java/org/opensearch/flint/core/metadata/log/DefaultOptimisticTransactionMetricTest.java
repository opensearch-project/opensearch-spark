/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.flint.common.metadata.log.FlintMetadataLog;
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry;

import java.util.Optional;
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState$;
import org.opensearch.flint.core.metrics.MetricsTestUtil;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DefaultOptimisticTransactionMetricTest {

  @Mock
  private FlintMetadataLog<FlintMetadataLogEntry> metadataLog;

  @Mock
  private FlintMetadataLogEntry logEntry;

  @InjectMocks
  private DefaultOptimisticTransaction<String> transaction;

  @Test
  void testCommitWithValidInitialCondition() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      when(metadataLog.getLatest()).thenReturn(Optional.of(logEntry));
      when(logEntry.state()).thenReturn(IndexState$.MODULE$.ACTIVE());

      transaction.initialLog(entry -> true)
          .transientLog(entry -> logEntry)
          .finalLog(entry -> logEntry)
          .commit(entry -> "Success");

      verify(metadataLog, times(2)).add(logEntry);

      verifier.assertHistoricGauge("indexState.updatedTo.active.count", 1);
    });
  }

  @Test
  void testConditionCheckFailed() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      when(metadataLog.getLatest()).thenReturn(Optional.of(logEntry));
      when(logEntry.state()).thenReturn(IndexState$.MODULE$.DELETED());

      transaction.initialLog(entry -> false)
          .finalLog(entry -> logEntry);

      assertThrows(IllegalStateException.class, () -> {
        transaction.commit(entry -> "Should Fail");
      });

      verifier.assertHistoricGauge("initialConditionCheck.failed.deleted.count", 1);
    });
  }
}
