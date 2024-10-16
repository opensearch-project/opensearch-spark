/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.List;
import java.util.Map;

/** AST node represent Parse with regex operation. */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@AllArgsConstructor
public class Parse extends UnresolvedPlan {
  /** Method used to parse a field. */
  private final ParseMethod parseMethod;

  /** Field. */
  private final UnresolvedExpression sourceField;

  /** Pattern. */
  private final Literal pattern;

  /** Optional arguments. */
  private final Map<String, Literal> arguments;

  /** Child Plan. */
  private UnresolvedPlan child;

  @Override
  public Parse attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitParse(this, context);
  }

  public static class Helper implements org.opensearch.sql.ast.tree.Help {

    public static final String HELP_TEXT =
            "PARSE Command Help\n" +
                    "\n" +
                    "Synopsis\n" +
                    "parse <field> <pattern>\n" +
                    "\n" +
                    "Description\n" +
                    "The 'parse' command parses a text field with a regular expression and appends the " +
                    "extracted values as new fields to the search result.\n" +
                    "\n" +
                    "Parameters\n" +
                    "1. field: MANDATORY\n" +
                    "   - Must be a text field\n" +
                    "   - Source field to be parsed\n" +
                    "\n" +
                    "2. pattern: MANDATORY\n" +
                    "   - String containing a regular expression pattern\n" +
                    "   - Used to extract new fields from the given text field\n" +
                    "   - If a new field name already exists, it will replace the original field\n" +
                    "\n" +
                    "Regular Expression Details\n" +
                    "- Uses Java regex engine to match the whole text field of each document\n" +
                    "- Each named capture group in the expression becomes a new STRING field\n" +
                    "- Pattern syntax: (?<fieldname>regex)\n" +
                    "\n" +
                    "Limitations\n" +
                    "1. Fields defined by parse cannot be parsed again\n" +
                    "   This will NOT work:\n" +
                    "   source=accounts | parse address '\\d+ (?<street>.+)' | parse street '\\w+ (?<road>\\w+)'\n" +
                    "\n" +
                    "2. Fields defined by parse cannot be overridden with other commands\n" +
                    "   This will NOT match any documents:\n" +
                    "   source=accounts | parse address '\\d+ (?<street>.+)' | eval street='1' | where street='1'\n" +
                    "\n" +
                    "3. The text field used by parse cannot be overridden\n" +
                    "   Parsing will fail if source field is overridden:\n" +
                    "   source=accounts | parse address '\\d+ (?<street>.+)' | eval address='1'\n" +
                    "\n" +
                    "4. Fields defined by parse cannot be filtered/sorted after using them in stats command\n" +
                    "   This where clause will NOT work:\n" +
                    "   source=accounts | parse email '.+@(?<host>.+)' | stats avg(age) by host | \n" +
                    "   where host=pyrami.com\n" +
                    "\n" +
                    "Notes\n" +
                    "- Parsing a null field will return an empty string\n" +
                    "- All extracted fields are of type STRING\n" +
                    "- Use cast() function to convert parsed fields to other types for comparison\n" +
                    "\n" +
                    "See Also\n" +
                    "- regex syntax\n" +
                    "- eval command\n" +
                    "- cast function";

    public static final String HELP_EXAMPLES =
            "Examples\n" +
                    "\n" +
                    "1. Create a new field:\n" +
                    "   Parse email to extract hostname:\n" +
                    "   source=accounts | parse email '.+@(?<host>.+)' | fields email, host\n" +
                    "\n" +
                    "2. Override existing field:\n" +
                    "   Remove street number from address:\n" +
                    "   source=accounts | parse address '\\d+ (?<address>.+)' | fields address\n" +
                    "\n" +
                    "3. Filter and sort by parsed field:\n" +
                    "   Extract and filter street numbers:\n" +
                    "   source=accounts | parse address '(?<streetNumber>\\d+) (?<street>.+)' | \n" +
                    "   where cast(streetNumber as int) > 500 | sort num(streetNumber) | \n" +
                    "   fields streetNumber, street";

    @Override
    public String describe() {
      return HELP_TEXT;
    }

    @Override
    public String sample() {
      return HELP_EXAMPLES;
    }
  }
}
