/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.common.grok.Grok;
import org.opensearch.sql.common.grok.GrokCompiler;
import org.opensearch.sql.common.grok.Match;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ParseUtils {
  private static final String NEW_FIELD_KEY = "new_field";

  /**
   * Construct corresponding ParseExpression by {@link ParseMethod}.
   *
   * @param parseMethod method used to parse
   * @param pattern pattern used for parsing
   * @param identifier derived field
   * @return {@link ParseExpression}
   */
  public static ParseExpression createParseExpression(
          ParseMethod parseMethod, String pattern, String identifier) {
    switch (parseMethod) {
      case GROK: return new GrokExpression(pattern, identifier);
      case PATTERNS: return new PatternsExpression(pattern, identifier);
      default: return new RegexExpression(pattern, identifier);
    }
  }

  /**
   * Get list of derived fields based on parse pattern.
   *
   * @param pattern pattern used for parsing
   * @return list of names of the derived fields
   */
  public static List<String> getNamedGroupCandidates(
          ParseMethod parseMethod, String pattern, Map<String, Literal> arguments) {
    switch (parseMethod) {
      case REGEX:
        return RegexExpression.getNamedGroupCandidates(pattern);
      case GROK:
        return GrokExpression.getNamedGroupCandidates(pattern);
      default:
        return PatternsExpression.getNamedGroupCandidates(
                arguments.containsKey(NEW_FIELD_KEY)
                        ? (String) arguments.get(NEW_FIELD_KEY).getValue()
                        : null);
    }
  }

  public static abstract class  ParseExpression {
    abstract String parseValue(String value);
  }
  
  public static class RegexExpression extends ParseExpression{
    private static final Pattern GROUP_PATTERN = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");
    private final Pattern regexPattern;
    protected final String identifier;

    public RegexExpression(String patterns, String identifier) {
      this.regexPattern = Pattern.compile(patterns);
      this.identifier = identifier;
    }

    /**
     * Get list of derived fields based on parse pattern.
     *
     * @param pattern pattern used for parsing
     * @return list of names of the derived fields
     */
    public static List<String> getNamedGroupCandidates(String pattern) {
      ImmutableList.Builder<String> namedGroups = ImmutableList.builder();
      Matcher m = GROUP_PATTERN.matcher(pattern);
      while (m.find()) {
        namedGroups.add(m.group(1));
      }
      return namedGroups.build();
    }

    @Override
    public String parseValue(String value)  {
      Matcher matcher = regexPattern.matcher(value);
      if (matcher.matches()) {
        return matcher.group(identifier);
      }
      return "";
    }
  }
  
  public static class GrokExpression extends ParseExpression{
    private static final GrokCompiler grokCompiler = GrokCompiler.newInstance();
    private final Grok grok;
    private final String identifier;

    public GrokExpression(String pattern, String identifier) {
      this.grok = grokCompiler.compile(pattern);
      this.identifier = identifier;
    }

    @Override
    public String parseValue(String value) {
      Match grokMatch = grok.match(value);
      Map<String, Object> capture = grokMatch.capture();
      Object match = capture.get(identifier);
      if (match != null) {
        return match.toString();
      }
      return "";
    }

    /**
     * Get list of derived fields based on parse pattern.
     *
     * @param pattern pattern used for parsing
     * @return list of names of the derived fields
     */
    public static List<String> getNamedGroupCandidates(String pattern) {
      Grok grok = grokCompiler.compile(pattern);
      return grok.namedGroups.stream()
              .map(grok::getNamedRegexCollectionById)
              .filter(group -> !group.equals("UNWANTED"))
              .collect(Collectors.toUnmodifiableList());
    }

  }
  
  public static class PatternsExpression extends ParseExpression{
    public static final String DEFAULT_NEW_FIELD = "patterns_field";

    private static final ImmutableSet<Character> DEFAULT_IGNORED_CHARS =
            ImmutableSet.copyOf(
                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
                            .chars()
                            .mapToObj(c -> (char) c)
                            .toArray(Character[]::new));
    private final boolean useCustomPattern;
    private Pattern pattern;

    /**
     * PatternsExpression.
     *
     * @param pattern pattern used for parsing
     * @param identifier derived field
     */
    public PatternsExpression(String pattern, String identifier) {
      useCustomPattern = !pattern.isEmpty();
      if (useCustomPattern) {
        this.pattern = Pattern.compile(pattern);
      }
    }

    @Override
    public String parseValue(String value)  {
      if (useCustomPattern) {
        return pattern.matcher(value).replaceAll("");
      }

      char[] chars = value.toCharArray();
      int pos = 0;
      for (int i = 0; i < chars.length; i++) {
        if (!DEFAULT_IGNORED_CHARS.contains(chars[i])) {
          chars[pos++] = chars[i];
        }
      }
      return new String(chars, 0, pos);
    }

    /**
     * Get list of derived fields.
     *
     * @param identifier identifier used to generate the field name
     * @return list of names of the derived fields
     */
    public static List<String> getNamedGroupCandidates(String identifier) {
      return ImmutableList.of(Objects.requireNonNullElse(identifier, DEFAULT_NEW_FIELD));
    }
  }
  
}
