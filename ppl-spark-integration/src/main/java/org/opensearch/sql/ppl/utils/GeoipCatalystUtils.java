/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute$;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar$;
import org.apache.spark.sql.catalyst.expressions.Alias$;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.CreateStruct;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual;
import org.apache.spark.sql.catalyst.expressions.LessThan;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.ScalaUDF;
import org.apache.spark.sql.catalyst.plans.logical.DataFrameDropColumns;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.expression.function.SerializableUdf;
import org.opensearch.sql.ppl.CatalystPlanContext;
import scala.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static org.opensearch.sql.ppl.utils.JoinSpecTransformer.join;

public interface GeoipCatalystUtils {

    String DEFAULT_GEOIP_TABLE_NAME = "geoip";
    String SOURCE_TABLE_ALIAS = "t1";
    String GEOIP_TABLE_ALIAS= "t2";

    static LogicalPlan getGeoipLogicalPlan(GeoIpParameters parameters, CatalystPlanContext context) {
        applyJoin(parameters.getIpAddress(), context);
        return applyProjection(parameters.getField(), parameters.getProperties(), context);
    }

    static LogicalPlan applyJoin(Expression ipAddress, CatalystPlanContext context) {
        return context.apply(left -> {
            LogicalPlan right = new UnresolvedRelation(seq(DEFAULT_GEOIP_TABLE_NAME), CaseInsensitiveStringMap.empty(), false);
            LogicalPlan leftAlias = SubqueryAlias$.MODULE$.apply(SOURCE_TABLE_ALIAS, left);
            LogicalPlan rightAlias = SubqueryAlias$.MODULE$.apply(GEOIP_TABLE_ALIAS, right);
            Optional<Expression> joinCondition = Optional.of(new And(
                new And(
                        new GreaterThanOrEqual(
                                getIpInt(ipAddress),
                                UnresolvedAttribute$.MODULE$.apply(seq(GEOIP_TABLE_ALIAS,"start"))
                        ),
                        new LessThan(
                                getIpInt(ipAddress),
                                UnresolvedAttribute$.MODULE$.apply(seq(GEOIP_TABLE_ALIAS,"end"))
                        )
                ),
                new EqualTo(
                        getIsIpv4(ipAddress),
                        UnresolvedAttribute$.MODULE$.apply(seq(GEOIP_TABLE_ALIAS,"ipv4"))
                )
            ));
            context.retainAllNamedParseExpressions(p -> p);
            context.retainAllPlans(p -> p);
            return join(leftAlias,
                    rightAlias,
                    Join.JoinType.INNER,
                    joinCondition,
                    new Join.JoinHint());
        });
    }

    static private LogicalPlan applyProjection(Expression field, List<String> properties, CatalystPlanContext context) {
        List<NamedExpression> projectExpressions = new ArrayList<>();
        projectExpressions.add(UnresolvedStar$.MODULE$.apply(Option.empty()));

        List<Expression> geoIpStructFields = createGeoIpStructFields(properties);
        Expression columnValue = (geoIpStructFields.size() == 1)?
                geoIpStructFields.get(0) : CreateStruct.apply(seq(geoIpStructFields));

        NamedExpression geoCol = Alias$.MODULE$.apply(
                columnValue,
                field.toString(),
                NamedExpression.newExprId(),
                seq(new java.util.ArrayList<>()),
                Option.empty(),
                seq(new java.util.ArrayList<>()));

        projectExpressions.add(geoCol);

        List<Expression> dropList = createGeoIpStructFields(new ArrayList<>());
        dropList.addAll(List.of(
                UnresolvedAttribute$.MODULE$.apply(seq(GEOIP_TABLE_ALIAS,"cidr")),
                UnresolvedAttribute$.MODULE$.apply(seq(GEOIP_TABLE_ALIAS,"start")),
                UnresolvedAttribute$.MODULE$.apply(seq(GEOIP_TABLE_ALIAS,"end")),
                UnresolvedAttribute$.MODULE$.apply(seq(GEOIP_TABLE_ALIAS,"ipv4"))
        ));

        context.apply(p -> new Project(seq(projectExpressions), p));
        return context.apply(p -> new DataFrameDropColumns(seq(dropList), p));
    }

    static private List<Expression> createGeoIpStructFields(List<String> attributeList) {
        List<String> attributeListToUse;
        if (attributeList == null || attributeList.isEmpty()) {
            attributeListToUse = List.of(
                    "country_iso_code",
                    "country_name",
                    "continent_name",
                    "region_iso_code",
                    "region_name",
                    "city_name",
                    "time_zone",
                    "location"
            );
        } else {
            attributeListToUse = attributeList;
        }

        return attributeListToUse.stream()
                .map(a -> UnresolvedAttribute$.MODULE$.apply(seq(
                        GEOIP_TABLE_ALIAS,
                        a.toLowerCase(Locale.ROOT)
                )))
                .collect(Collectors.toList());
    }

    static private Expression getIpInt(Expression ipAddress) {
        return new ScalaUDF(SerializableUdf.ipToInt,
                DataTypes.createDecimalType(38,0),
                seq(ipAddress),
                seq(),
                Option.empty(),
                Option.apply("ip_to_int"),
                false,
                true
        );
    }

    static private Expression getIsIpv4(Expression ipAddress) {
        return new ScalaUDF(SerializableUdf.isIpv4,
                DataTypes.BooleanType,
                seq(ipAddress),
                seq(), Option.empty(),
                Option.apply("is_ipv4"),
                false,
                true
        );
    }

    @Getter
    @AllArgsConstructor
    class GeoIpParameters {
        private final Expression field;
        private final Expression ipAddress;
        private final List<String> properties;
    }
}
