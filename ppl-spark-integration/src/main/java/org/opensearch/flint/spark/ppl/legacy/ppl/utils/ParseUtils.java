/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.ppl.utils;

import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.RegExpExtract;
import org.apache.spark.sql.catalyst.expressions.RegExpReplace;
import org.opensearch.flint.spark.ppl.legacy.ast.expression.Literal;
import org.opensearch.flint.spark.ppl.legacy.ast.expression.ParseMethod;
import org.opensearch.flint.spark.ppl.legacy.common.grok.Grok;
import org.opensearch.flint.spark.ppl.legacy.common.grok.GrokCompiler;
import org.opensearch.flint.spark.ppl.legacy.common.grok.Match;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.opensearch.flint.spark.ppl.legacy.common.grok.GrokUtils.getGroupPatternName;

public class ParseUtils {
    private static final Pattern GROUP_PATTERN = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");
    private static final String NEW_FIELD_KEY = "new_field";


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
                return RegexExpression.getNamedGroupCandidates(pattern, arguments);
            case GROK:
                return GrokExpression.getNamedGroupCandidates(pattern, arguments);
            default:
                return PatternsExpression.getNamedGroupCandidates(pattern, arguments);
        }
    }

    /**
     * Get list of derived fields based on parse pattern.
     *
     * @param pattern pattern used for parsing
     * @return list of names of the derived fields
     */
    public static int getNamedGroupIndex(
            ParseMethod parseMethod, String pattern, String namedGroup, Map<String, Literal> arguments) {
        switch (parseMethod) {
            case REGEX:
                return RegexExpression.getNamedGroupIndex(pattern, namedGroup, arguments);
            case GROK:
                return GrokExpression.getNamedGroupIndex(pattern, namedGroup, arguments);
            default:
                return PatternsExpression.getNamedGroupIndex(pattern, namedGroup, arguments);
        }
    }

    /**
     * extract the cleaner pattern without the additional fields
     *
     * @param parseMethod
     * @param pattern
     * @param columns
     * @return
     */
    public static String extractPatterns(
            ParseMethod parseMethod, String pattern, List<String> columns) {
        switch (parseMethod) {
            case REGEX:
                return RegexExpression.extractPattern(pattern, columns);
            case GROK:
                return GrokExpression.extractPattern(pattern, columns);
            default:
                return PatternsExpression.extractPattern(pattern, columns);
        }
    }

    public static Expression getRegExpCommand(ParseMethod parseMethod, Expression sourceField,
                                              org.apache.spark.sql.catalyst.expressions.Literal patternLiteral,
                                              org.apache.spark.sql.catalyst.expressions.Literal groupIndexLiteral) {
        switch (parseMethod) {
            case REGEX:
                return RegexExpression.getRegExpCommand(sourceField, patternLiteral, groupIndexLiteral);
            case GROK:
                return GrokExpression.getRegExpCommand(sourceField, patternLiteral, groupIndexLiteral);
            default:
                return PatternsExpression.getRegExpCommand(sourceField, patternLiteral, groupIndexLiteral);
        }
    }

    public static class RegexExpression {
        /**
         * Get list of derived fields based on parse pattern.
         *
         * @param pattern   pattern used for parsing
         * @param arguments
         * @return list of names of the derived fields
         */
        public static List<String> getNamedGroupCandidates(String pattern, Map<String, Literal> arguments) {
            ImmutableList.Builder<String> namedGroups = ImmutableList.builder();
            Matcher m = GROUP_PATTERN.matcher(pattern);
            while (m.find()) {
                namedGroups.add(m.group(1));
            }
            return namedGroups.build();
        }

        public static int getNamedGroupIndex(String pattern, String groupName, Map<String, Literal> arguments) {
            List<String> groupCandidates = getNamedGroupCandidates(pattern, arguments);
            for (int i = 0; i < groupCandidates.size(); i++) {
                if (groupCandidates.get(i).equals(groupName)) return i;
            }
            return -1;
        }

        public static Expression getRegExpCommand(Expression sourceField,
                                                  org.apache.spark.sql.catalyst.expressions.Literal patternLiteral,
                                                  org.apache.spark.sql.catalyst.expressions.Literal groupIndexLiteral) {
            return new RegExpExtract(sourceField, patternLiteral, groupIndexLiteral);
        }

        public static String extractPattern(String patterns, List<String> columns) {
            return patterns;
        }
    }

    public static class GrokExpression {
        private static final GrokCompiler grokCompiler = GrokCompiler.newInstance();

        public static Expression getRegExpCommand(Expression sourceField, org.apache.spark.sql.catalyst.expressions.Literal patternLiteral, org.apache.spark.sql.catalyst.expressions.Literal groupIndexLiteral) {
            return new RegExpExtract(sourceField, patternLiteral, groupIndexLiteral);
        }

        /**
         * Get list of derived fields based on parse pattern.
         *
         * @param pattern   pattern used for parsing
         * @param arguments
         * @return list of names of the derived fields
         */
        public static List<String> getNamedGroupCandidates(String pattern, Map<String, Literal> arguments) {
            Grok grok = grokCompiler.compile(pattern);
            return grok.namedGroups.stream()
                    .map(grok::getNamedRegexCollectionById)
                    .filter(group -> !group.equals("UNWANTED"))
                    .collect(Collectors.toUnmodifiableList());
        }

        public static int getNamedGroupIndex(String pattern, String groupName, Map<String, Literal> arguments) {
            String name = getGroupPatternName(grokCompiler.compile(pattern), groupName);
            List<String> namedGroups = new ArrayList<>(grokCompiler.compile(pattern).namedGroups);
            for (int i = 0; i < namedGroups.size(); i++) {
                if (namedGroups.get(i).equals(name)) return i;
            }
            return -1;
        }

        public static String extractPattern(final String patterns, List<String> columns) {
            Grok grok = grokCompiler.compile(patterns);
            return grok.getNamedRegex();
        }
    }

    public static class PatternsExpression  {
        public static final String DEFAULT_NEW_FIELD = "patterns_field";
        private static final String DEFAULT_IGNORED_PATTERN = "[a-zA-Z0-9]";


        public static int getNamedGroupIndex(String pattern, String namedGroup, Map<String, Literal> arguments) {
            return 0;
        }

        public static Expression getRegExpCommand(Expression sourceField,
                                                  org.apache.spark.sql.catalyst.expressions.Literal patternLiteral,
                                                  org.apache.spark.sql.catalyst.expressions.Literal groupIndexLiteral) {
            return new RegExpReplace(sourceField, patternLiteral, org.apache.spark.sql.catalyst.expressions.Literal.create("", StringType));
        }
        
        /**
         * Get list of derived fields.
         *
         * @param pattern
         * @param arguments
         * @return list of names of the derived fields
         */
        public static List<String> getNamedGroupCandidates(String pattern, Map<String, Literal> arguments) {
            return ImmutableList.of(arguments.containsKey(NEW_FIELD_KEY) ? arguments.get(NEW_FIELD_KEY).toString() : DEFAULT_NEW_FIELD);
        }

        public static String extractPattern(String patterns, List<String> columns) {
            return patterns != null && !patterns.isEmpty() ? patterns : DEFAULT_IGNORED_PATTERN;
        }
    }

}
