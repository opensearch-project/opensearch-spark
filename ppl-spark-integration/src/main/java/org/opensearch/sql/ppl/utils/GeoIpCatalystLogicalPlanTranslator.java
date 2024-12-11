/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.spark.SparkEnv;
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

public interface GeoIpCatalystLogicalPlanTranslator {
    String SPARK_CONF_KEY = "spark.geoip.tablename";
    String DEFAULT_GEOIP_TABLE_NAME = "geoip";
    String SOURCE_TABLE_ALIAS = "t1";
    String GEOIP_TABLE_ALIAS = "t2";
    List<String> GEOIP_TABLE_COLUMNS = List.of(
            "country_iso_code",
            "country_name",
            "continent_name",
            "region_iso_code",
            "region_name",
            "city_name",
            "time_zone",
            "location"
    );

    /**
     * Responsible to produce a Spark Logical Plan with given GeoIp command arguments, below is the sample logical plan
     * with configuration [source=users, field=a, ipAddress=ip, properties=[country_name, city_name]]
     * +- 'DataFrameDropColumns ['t2.country_iso_code, 't2.country_name, 't2.continent_name, 't2.region_iso_code, 't2.region_name, 't2.city_name, 't2.time_zone, 't2.location, 't2.cidr, 't2.start, 't2.end, 't2.ipv4]
     * -- +- 'Project [*, named_struct(country_name, 't2.country_name, city_name, 't2.city_name) AS a#0]
     * -- -- +- 'Join LeftOuter, (((ip_to_int('ip) >= 't2.start) AND (ip_to_int('ip) < 't2.end)) AND (is_ipv4('ip) = 't2.ipv4))
     * -- -- -- :- 'SubqueryAlias t1
     * -- -- -- -- :  +- 'UnresolvedRelation [users], [], false
     * -- -- -- +- 'SubqueryAlias t2
     * -- -- -- -- -- +- 'UnresolvedRelation [geoip], [], false
     * .
     * And the corresponded SQL query:
     * .
     * SELECT users.*, struct(geoip.country_name, geoip.city_name) AS a
     * FROM users, geoip
     * WHERE geoip.ip_range_start <= ip_to_int(users.ip)
     *   AND geoip.ip_range_end > ip_to_int(users.ip)
     *   AND geoip.ip_type = is_ipv4(users.ip);
     *
     * @param parameters GeoIp function parameters.
     * @param context Context instance to retrieved Expression in resolved form.
     * @return a LogicalPlan which will project new col with geoip location based on given ipAddresses.
     */
    static LogicalPlan getGeoipLogicalPlan(GeoIpParameters parameters, CatalystPlanContext context) {
        applyJoin(parameters.getIpAddress(), context);
        return applyProjection(parameters.getField(), parameters.getProperties(), context);
    }

    /**
     * Responsible to produce join plan for GeoIp command, below is the sample logical plan
     * with configuration [source=users, ipAddress=ip]
     * +- 'Join LeftOuter, (((ip_to_int('ip) >= 't2.start) AND (ip_to_int('ip) < 't2.end)) AND (is_ipv4('ip) = 't2.ipv4))
     * -- :- 'SubqueryAlias t1
     * -- -- :  +- 'UnresolvedRelation [users], [], false
     * -- +- 'SubqueryAlias t2
     * -- -- -- +- 'UnresolvedRelation [geoip], [], false
     *
     * @param ipAddress Expression representing ip addresses to be queried.
     * @param context Context instance to retrieved Expression in resolved form.
     * @return a LogicalPlan which will perform join based on ip within cidr range in geoip table.
     */
    static private LogicalPlan applyJoin(Expression ipAddress, CatalystPlanContext context) {
        return context.apply(left -> {
            LogicalPlan right = new UnresolvedRelation(seq(getGeoipTableName()), CaseInsensitiveStringMap.empty(), false);
            LogicalPlan leftAlias = SubqueryAlias$.MODULE$.apply(SOURCE_TABLE_ALIAS, left);
            LogicalPlan rightAlias = SubqueryAlias$.MODULE$.apply(GEOIP_TABLE_ALIAS, right);
            Optional<Expression> joinCondition = Optional.of(new And(
                    new And(
                            new GreaterThanOrEqual(
                                    getIpInt(ipAddress),
                                    UnresolvedAttribute$.MODULE$.apply(seq(GEOIP_TABLE_ALIAS,"ip_range_start"))
                            ),
                            new LessThan(
                                    getIpInt(ipAddress),
                                    UnresolvedAttribute$.MODULE$.apply(seq(GEOIP_TABLE_ALIAS,"ip_range_end"))
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
                    Join.JoinType.LEFT,
                    joinCondition,
                    new Join.JoinHint());
        });
    }

    /**
     * Responsible to produce a Spark Logical Plan with given GeoIp command arguments, below is the sample logical plan
     * with configuration [source=users, field=a, properties=[country_name, city_name]]
     * +- 'DataFrameDropColumns ['t2.country_iso_code, 't2.country_name, 't2.continent_name, 't2.region_iso_code, 't2.region_name, 't2.city_name, 't2.time_zone, 't2.location, 't2.cidr, 't2.start, 't2.end, 't2.ipv4]
     * -- +- 'Project [*, named_struct(country_name, 't2.country_name, city_name, 't2.city_name) AS a#0]
     *
     * @param field Name of new eval geoip column.
     * @param properties List of geo properties to be returned.
     * @param context Context instance to retrieved Expression in resolved form.
     * @return a LogicalPlan which will return source table and new eval geoip column.
     */
    static private LogicalPlan applyProjection(String field, List<String> properties, CatalystPlanContext context) {
        List<NamedExpression> projectExpressions = new ArrayList<>();
        projectExpressions.add(UnresolvedStar$.MODULE$.apply(Option.empty()));

        List<Expression> geoIpStructFields = createGeoIpStructFields(properties);
        Expression columnValue = (geoIpStructFields.size() == 1)?
                geoIpStructFields.get(0) : CreateStruct.apply(seq(geoIpStructFields));

        NamedExpression geoCol = Alias$.MODULE$.apply(
                columnValue,
                field,
                NamedExpression.newExprId(),
                seq(new ArrayList<>()),
                Option.empty(),
                seq(new ArrayList<>()));

        projectExpressions.add(geoCol);

        List<Expression> dropList = createGeoIpStructFields(new ArrayList<>());
        dropList.addAll(List.of(
                UnresolvedAttribute$.MODULE$.apply(seq(GEOIP_TABLE_ALIAS,"cidr")),
                UnresolvedAttribute$.MODULE$.apply(seq(GEOIP_TABLE_ALIAS,"ip_range_start")),
                UnresolvedAttribute$.MODULE$.apply(seq(GEOIP_TABLE_ALIAS,"ip_range_end")),
                UnresolvedAttribute$.MODULE$.apply(seq(GEOIP_TABLE_ALIAS,"ipv4"))
        ));

        context.apply(p -> new Project(seq(projectExpressions), p));
        return context.apply(p -> new DataFrameDropColumns(seq(dropList), p));
    }

    static private List<Expression> createGeoIpStructFields(List<String> attributeList) {
        List<String> attributeListToUse;
        if (attributeList == null || attributeList.isEmpty()) {
            attributeListToUse = GEOIP_TABLE_COLUMNS;
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
        return new ScalaUDF(SerializableUdf.geoIpUtils.ipToInt,
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
        return new ScalaUDF(SerializableUdf.geoIpUtils.isIpv4,
                DataTypes.BooleanType,
                seq(ipAddress),
                seq(), Option.empty(),
                Option.apply("is_ipv4"),
                false,
                true
        );
    }

    static private String getGeoipTableName() {
        String tableName = DEFAULT_GEOIP_TABLE_NAME;

        if (SparkEnv.get() != null && SparkEnv.get().conf() != null) {
            tableName = SparkEnv.get().conf().get(SPARK_CONF_KEY, DEFAULT_GEOIP_TABLE_NAME);
        }

        return tableName;
    }

    @Getter
    @AllArgsConstructor
    class GeoIpParameters {
        private final String field;
        private final Expression ipAddress;
        private final List<String> properties;
    }

    enum GeoIpProperty {
        COUNTRY_ISO_CODE,
        COUNTRY_NAME,
        CONTINENT_NAME,
        REGION_ISO_CODE,
        REGION_NAME,
        CITY_NAME,
        TIME_ZONE,
        LOCATION
    }
}
