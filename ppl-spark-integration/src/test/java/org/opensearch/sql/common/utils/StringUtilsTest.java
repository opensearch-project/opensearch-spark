package org.opensearch.sql.common.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.common.utils.StringUtils.unquoteIdentifier;
import static org.opensearch.sql.common.utils.StringUtils.unquoteText;

import org.junit.Test;

public class StringUtilsTest {
  @Test
  public void unquoteIdentifierTest() {
    assertEquals("test", unquoteIdentifier("test"));
    assertEquals("test", unquoteIdentifier("`test`"));
    
    assertEquals("\"test\"", unquoteIdentifier("\"test\""));
    assertEquals("\'test\'", unquoteIdentifier("\'test\'"));
  }

  @Test
  public void unquoteTextTest() {
    assertEquals("test", unquoteText("test"));
    assertEquals("test", unquoteText("'test'"));

    assertEquals("test'", unquoteText("'test'''"));
    assertEquals("test\"", unquoteText("\"test\"\"\""));

    assertEquals("te``st", unquoteText("'te``st'"));
    assertEquals("te``st", unquoteText("\"te``st\""));

    assertEquals("te'st", unquoteText("'te''st'"));
    assertEquals("te''st", unquoteText("\"te''st\""));

    assertEquals("te\"\"st", unquoteText("'te\"\"st'"));
    assertEquals("te\"st", unquoteText("\"te\"\"st\""));

    assertEquals("''", unquoteText("''''''"));
    assertEquals("\"\"", unquoteText("\"\"\"\"\"\""));

    assertEquals("test'", unquoteText("'test''"));

    assertEquals("", unquoteText(""));
    assertEquals("'", unquoteText("'"));
    assertEquals("\"", unquoteText("\""));

    assertEquals("hello'", unquoteText("'hello''"));
    assertEquals("don't", unquoteText("'don't'"));
    assertEquals("don\"t", unquoteText("\"don\"t\""));

    assertEquals("hel\\lo'", unquoteText("'hel\\lo''"));
    assertEquals("hel'lo", unquoteText("'hel'lo'"));
    assertEquals("hel\"lo", unquoteText("\"hel\"lo\""));
    assertEquals("hel\\'\\lo", unquoteText("'hel\\\\''\\\\lo'"));
  }
}
