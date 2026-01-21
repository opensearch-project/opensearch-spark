/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.common.grok;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.flint.spark.ppl.legacy.common.grok.exception.GrokException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static org.opensearch.flint.spark.ppl.legacy.common.grok.DefaultPatterns.withDefaultPatterns;

public class GrokCompiler implements Serializable {
  
  /** {@code Grok} patterns definitions. */
  private final Map<String, String> grokPatternDefinitions = withDefaultPatterns(new HashMap<>());

  private GrokCompiler() {}

  public static GrokCompiler newInstance() {
    return new GrokCompiler();
  }

  /** Compiles a given Grok pattern and returns a Grok object which can parse the pattern. */
  public Grok compile(String pattern) throws IllegalArgumentException {
    return compile(pattern, false);
  }

  public Grok compile(final String pattern, boolean namedOnly) throws IllegalArgumentException {
    return compile(pattern, ZoneOffset.systemDefault(), namedOnly);
  }

  /**
   * Compiles a given Grok pattern and returns a Grok object which can parse the pattern.
   *
   * @param pattern : Grok pattern (ex: %{IP})
   * @param defaultTimeZone : time zone used to parse a timestamp when it doesn't contain the time
   *     zone
   * @param namedOnly : Whether to capture named expressions only or not (i.e. %{IP:ip} but not
   *     ${IP})
   * @return a compiled pattern
   * @throws IllegalArgumentException when pattern definition is invalid
   */
  public Grok compile(final String pattern, ZoneId defaultTimeZone, boolean namedOnly)
      throws IllegalArgumentException {

    if (StringUtils.isBlank(pattern)) {
      throw new IllegalArgumentException("{pattern} should not be empty or null");
    }

    String namedRegex = pattern;
    int index = 0;
    // flag for infinite recursion
    int iterationLeft = 1000;
    Boolean continueIteration = true;
    Map<String, String> patternDefinitions = new HashMap<>(grokPatternDefinitions);

    // output
    Map<String, String> namedRegexCollection = new HashMap<>();

    // Replace %{foo} with the regex (mostly group name regex)
    // and then compile the regex
    while (continueIteration) {
      continueIteration = false;
      if (iterationLeft <= 0) {
        throw new IllegalArgumentException("Deep recursion pattern compilation of " + pattern);
      }
      iterationLeft--;

      Set<String> namedGroups = GrokUtils.getNameGroups(GrokUtils.GROK_PATTERN.pattern());
      Matcher matcher = GrokUtils.GROK_PATTERN.matcher(namedRegex);
      // Match %{Foo:bar} -> pattern name and subname
      // Match %{Foo=regex} -> add new regex definition
      if (matcher.find()) {
        continueIteration = true;
        Map<String, String> group = GrokUtils.namedGroups(matcher, namedGroups);
        if (group.get("definition") != null) {
          patternDefinitions.put(group.get("pattern"), group.get("definition"));
          group.put("name", group.get("name") + "=" + group.get("definition"));
        }
        int count = StringUtils.countMatches(namedRegex, "%{" + group.get("name") + "}");
        for (int i = 0; i < count; i++) {
          String definitionOfPattern = patternDefinitions.get(group.get("pattern"));
          if (definitionOfPattern == null) {
            throw new IllegalArgumentException(
                format("No definition for key '%s' found, aborting", group.get("pattern")));
          }
          String replacement = String.format("(?<name%d>%s)", index, definitionOfPattern);
          if (namedOnly && group.get("subname") == null) {
            replacement = String.format("(?:%s)", definitionOfPattern);
          }
          namedRegexCollection.put(
              "name" + index,
              (group.get("subname") != null ? group.get("subname") : group.get("name")));
          namedRegex =
              StringUtils.replace(namedRegex, "%{" + group.get("name") + "}", replacement, 1);
          // System.out.println(_expanded_pattern);
          index++;
        }
      }
    }

    if (namedRegex.isEmpty()) {
      throw new IllegalArgumentException("Pattern not found");
    }

    return new Grok(pattern, namedRegex, namedRegexCollection, patternDefinitions, defaultTimeZone);
  }
}
