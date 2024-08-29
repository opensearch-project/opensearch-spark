package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.analysis.UnresolvedStar$;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.RegExpExtract;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ppl.CatalystPlanContext;
import scala.Option;
import scala.collection.Seq;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;

public interface ParseStrategy {
    /**
     * transform the parse/grok/patterns command into a standard catalyst RegExpExtract expression  
     * Since spark's RegExpExtract cant accept actual regExp group name we need to translate the group's name into its corresponding index
     * 
     * @param node
     * @param sourceField
     * @param parseMethod
     * @param arguments
     * @param pattern
     * @param context
     * @return
     */
    static LogicalPlan visitParseCommand(Parse node, Expression sourceField, ParseMethod parseMethod, Map<String, Literal> arguments, String pattern, CatalystPlanContext context) {
        Map<String, Integer> namedGroupNumbers = new LinkedHashMap<>();
        List<String> namedGroupCandidates = ParseUtils.getNamedGroupCandidates(parseMethod, pattern, arguments);
        String cleanedPattern = ParseUtils.extractPatterns(parseMethod, pattern, namedGroupCandidates);
        //get projected fields names (ignore the rest)
        context.getProjectedFields().forEach(field -> {
            // in select * case - take all namedGroupCandidates
            if(field instanceof AllFields) {
                for (int i = 0; i < namedGroupCandidates.size(); i++) {
                    namedGroupNumbers.put(namedGroupCandidates.get(i),
                            ParseUtils.getNamedGroupIndex(parseMethod, pattern, namedGroupCandidates.get(i)));
                }
                // in specific field case - match to the namedGroupCandidates group
            } else for (int i = 0; i < namedGroupCandidates.size(); i++) {
                if (((Field)field).getField().toString().equals(namedGroupCandidates.get(i))) {
                    namedGroupNumbers.put(namedGroupCandidates.get(i),
                            ParseUtils.getNamedGroupIndex(parseMethod, pattern, namedGroupCandidates.get(i)));
                }
            }
        });
        //list the group numbers of these projected fields
        // match the regExpExtract group identifier with its number
        namedGroupNumbers.forEach((group, index) -> {
            //first create the regExp 
            RegExpExtract regExpExtract = new RegExpExtract(sourceField,
                    org.apache.spark.sql.catalyst.expressions.Literal.create(cleanedPattern, StringType),
                    org.apache.spark.sql.catalyst.expressions.Literal.create(index + 1, IntegerType));
            //next Alias the extracted fields
            context.getNamedParseExpressions().push(
                    org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(regExpExtract,
                            group,
                            NamedExpression.newExprId(),
                            seq(new java.util.ArrayList<String>()),
                            Option.empty(),
                            seq(new java.util.ArrayList<String>())));
        });

        // Create an UnresolvedStar for all-fields projection (possible external wrapping projection that may include additional fields)
        context.getNamedParseExpressions().push(UnresolvedStar$.MODULE$.apply(Option.<Seq<String>>empty()));
        // extract all fields to project with
        Seq<NamedExpression> projectExpressions = context.retainAllNamedParseExpressions(p -> (NamedExpression) p);
        // build the plan with the projection step
        LogicalPlan child = context.apply(p -> new org.apache.spark.sql.catalyst.plans.logical.Project(projectExpressions, p));
        return child;
    }

}
