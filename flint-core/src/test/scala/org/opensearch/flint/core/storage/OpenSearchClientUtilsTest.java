package org.opensearch.flint.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.opensearch.flint.core.FlintOptions;

class OpenSearchClientUtilsTest {

  @Test
  public void testGetServiceNameForSigV4() {
    assertEquals("es",
        OpenSearchClientUtils.getServiceNameForSigV4(getFlintOptions(FlintOptions.INDEX_TYPE_AOS)));

    assertEquals("aoss", OpenSearchClientUtils.getServiceNameForSigV4(
        getFlintOptions(FlintOptions.INDEX_TYPE_AOSS)));

    assertThrows(NullPointerException.class, () -> OpenSearchClientUtils.getServiceNameForSigV4(
        getFlintOptions("INVALID_INDEX_TYPE")));
  }

  private FlintOptions getFlintOptions(String indexType) {
    return new FlintOptions(ImmutableMap.of(FlintOptions.INDEX_TYPE, indexType));
  }
}
