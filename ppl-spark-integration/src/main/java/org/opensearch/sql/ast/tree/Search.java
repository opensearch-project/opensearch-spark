package org.opensearch.sql.ast.tree;

public class Search {
    public static class Helper implements org.opensearch.sql.ast.tree.Help {
        public static final String HELP_SAMPLES =
                "SEARCH Command Examples\n" +
                        "                    \"1. Basic search with filtering:\\n\" +\n" +
                        "                    \"   search source=logs | where status = 404\\n\" +\n" +
                        "                    \"\\n\" +\n" +
                        "                    \"2. Search with multiple piped commands:\\n\" +\n" +
                        "                    \"   search source=metrics | stats avg(cpu_usage) by host | sort avg(cpu_usage) desc\\n\" +\n" +
                        "                    \"\\n\" +\n" +
                        "                    \"3. Implicit search (omitting 'search' keyword):\\n\" +\n" +
                        "                    \"   source=web_logs | where response_time > 1000 | fields url, response_time\\n\" +\n" +
                        "                    \"\\n\" +\n" +
                        "                    \"4. Search with pattern matching:\\n\" +\n" +
                        "                    \"   search source=events | where message like '%error%'\\n\" +\n" +
                        "                    \"\\n\" +\n";

        public static final String HELP_TEXT =
                "SEARCH Command Help\n" +
                        "\n" +
                        "Synopsis\n" +
                        "search [source=<index-name>] [<filter_expression>] | [<command_1>] | [<command_2>] | ... | [<command_n>]\n" +
                        "\n" +
                        "Description\n" +
                        "The 'search' command retrieves and filters data from specified OpenSearch indices. " +
                        "It can be combined with other commands using pipes (|) to perform complex data transformations and analytics.\n" +
                        "\n" +
                        "Syntax Elements\n" +
                        "\n" +
                        "1. Source Specification\n" +
                        "   - source=<index-name>: Specifies the index to search from\n" +
                        "   - Multiple indices can be specified using wildcards: source=index-*\n" +
                        "\n" +
                        "2. Filter Expression\n" +
                        "   - Optional logical expression to filter the data\n" +
                        "   - Can be placed before or after the source specification\n" +
                        "\n" +
                        "3. Piped Commands\n" +
                        "   - Additional commands can be chained using the pipe symbol (|)\n" +
                        "   - Each command processes the output of the previous command\n" +
                        "\n" +
                        "Options\n" +
                        "The 'search' command can be implicit or explicit at the beginning of a query.\n" +
                        "\n" +
                        "Common Piped Commands\n" +
                        "- where: Filters results based on conditions\n" +
                        "- fields: Selects specific fields to include in the output\n" +
                        "- stats: Performs statistical aggregations\n" +
                        "- sort: Orders results based on specified fields\n" +
                        "- head/tail: Limits the number of results returned\n" +
                        "- eval: Creates new fields based on expressions\n" +
                        "- rename: Renames fields in the output\n" +
                        "\n" +
                        "Notes\n" +
                        "- The order of filter expressions and source specification is flexible\n" +
                        "- Field names are case-sensitive\n" +
                        "- Use quotes for string values containing spaces or special characters\n" +
                        "\n";

        @Override
        public String describe() {
            return HELP_TEXT;
        }

        @Override
        public String sample() {
            return HELP_SAMPLES;
        }
    }
}
