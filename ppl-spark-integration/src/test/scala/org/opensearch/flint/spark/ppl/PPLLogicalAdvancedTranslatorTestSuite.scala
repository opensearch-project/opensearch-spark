/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.junit.Assert.assertEquals
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Descending, Divide, EqualTo, Floor, GreaterThan, GreaterThanOrEqual, LessThan, Like, Literal, SortOrder, UnixTimestamp}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._

class PPLLogicalAdvancedTranslatorTestSuite
    extends SparkFunSuite
    with LogicalPlanTestUtils
    with Matchers {

  private val planTrnasformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  ignore("Find What are the average prices for different types of properties") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
      plan(pplParser, "source = housing_properties | stats avg(price) by property_type", false),
      context)
    // SQL: SELECT property_type, AVG(price) FROM housing_properties GROUP BY property_type
    val table = UnresolvedRelation(Seq("housing_properties"))

    val avgPrice = Alias(Average(UnresolvedAttribute("price")), "avg(price)")()
    val propertyType = UnresolvedAttribute("property_type")
    val grouped = Aggregate(Seq(propertyType), Seq(propertyType, avgPrice), table)

    val projectList = Seq(
      UnresolvedAttribute("property_type"),
      Alias(Average(UnresolvedAttribute("price")), "avg(price)")())
    val expectedPlan = Project(projectList, grouped)

    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "???")

  }

  ignore(
    "Find the top 10 most expensive properties in California, including their addresses, prices, and cities") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
      plan(
        pplParser,
        "source = housing_properties | where state = `CA` | fields address, price, city | sort - price | head 10",
        false),
      context)
    // SQL: SELECT address, price, city FROM housing_properties WHERE state = 'CA' ORDER BY price DESC LIMIT 10

    // Constructing the expected Catalyst Logical Plan
    val table = UnresolvedRelation(Seq("housing_properties"))
    val filter = Filter(EqualTo(UnresolvedAttribute("state"), Literal("CA")), table)
    val projectList = Seq(
      UnresolvedAttribute("address"),
      UnresolvedAttribute("price"),
      UnresolvedAttribute("city"))
    val projected = Project(projectList, filter)
    val sortOrder = SortOrder(UnresolvedAttribute("price"), Descending) :: Nil
    val sorted = Sort(sortOrder, true, projected)
    val limited = Limit(Literal(10), sorted)
    val finalProjectList = Seq(
      UnresolvedAttribute("address"),
      UnresolvedAttribute("price"),
      UnresolvedAttribute("city"))

    val expectedPlan = Project(finalProjectList, limited)

    // Assert that the generated plan is as expected
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "???")
  }

  ignore("Find the average price per unit of land space for properties in different cities") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
      plan(
        pplParser,
        "source = housing_properties | where land_space > 0 | eval price_per_land_unit = price / land_space | stats avg(price_per_land_unit) by city",
        false),
      context)
    // SQL: SELECT city, AVG(price / land_space) AS avg_price_per_land_unit FROM housing_properties WHERE land_space > 0 GROUP BY city
    val table = UnresolvedRelation(Seq("housing_properties"))
    val filter = Filter(GreaterThan(UnresolvedAttribute("land_space"), Literal(0)), table)
    val expression = AggregateExpression(
      Average(Divide(UnresolvedAttribute("price"), UnresolvedAttribute("land_space"))),
      mode = Complete,
      isDistinct = false)
    val aggregateExpr = Alias(expression, "avg_price_per_land_unit")()
    val groupBy = Aggregate(
      groupingExpressions = Seq(UnresolvedAttribute("city")),
      aggregateExpressions = Seq(aggregateExpr),
      filter)

    val expectedPlan = Project(
      projectList =
        Seq(UnresolvedAttribute("city"), UnresolvedAttribute("avg_price_per_land_unit")),
      groupBy)
    // Continue with your test...
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "???")
  }

  ignore("Find the houses posted in the last month, how many are still for sale") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
      plan(
        pplParser,
        "search source=housing_properties | where listing_age >= 0 | where listing_age < 30 | stats count() by property_status",
        false),
      context)
    // SQL: SELECT property_status, COUNT(*) FROM housing_properties WHERE listing_age >= 0 AND listing_age < 30 GROUP BY property_status;

    val filter = Filter(
      LessThan(UnresolvedAttribute("listing_age"), Literal(30)),
      Filter(
        GreaterThanOrEqual(UnresolvedAttribute("listing_age"), Literal(0)),
        UnresolvedRelation(Seq("housing_properties"))))

    val expression = AggregateExpression(Count(Literal(1)), mode = Complete, isDistinct = false)

    val aggregateExpressions = Seq(Alias(expression, "count")())

    val groupByAttributes = Seq(UnresolvedAttribute("property_status"))
    val expectedPlan = Aggregate(groupByAttributes, aggregateExpressions, filter)
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "???")
  }

  ignore(
    "Find all the houses listed by agency Compass in  decreasing price order. Also provide only price, address and agency name information.") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
      plan(
        pplParser,
        "source = housing_properties | where match( agency_name , `Compass` ) | fields address , agency_name , price | sort - price ",
        false),
      context)
    // SQL: SELECT address, agency_name, price FROM housing_properties WHERE agency_name LIKE '%Compass%' ORDER BY price DESC

    val projectList = Seq(
      UnresolvedAttribute("address"),
      UnresolvedAttribute("agency_name"),
      UnresolvedAttribute("price"))
    val table = UnresolvedRelation(Seq("housing_properties"))

    val filterCondition = Like(UnresolvedAttribute("agency_name"), Literal("%Compass%"), '\\')
    val filter = Filter(filterCondition, table)

    val sortOrder = Seq(SortOrder(UnresolvedAttribute("price"), Descending))
    val sort = Sort(sortOrder, true, filter)

    val expectedPlan = Project(projectList, sort)
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "???")
  }

  ignore("Find details of properties owned by Zillow with at least 3 bedrooms and 2 bathrooms") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
      plan(
        pplParser,
        "source = housing_properties | where is_owned_by_zillow = 1 and bedroom_number >= 3 and bathroom_number >= 2 | fields address, price, city, listing_age",
        false),
      context)
    // SQL:SELECT address, price, city, listing_age FROM housing_properties WHERE is_owned_by_zillow = 1 AND bedroom_number >= 3 AND bathroom_number >= 2;
    val projectList = Seq(
      UnresolvedAttribute("address"),
      UnresolvedAttribute("price"),
      UnresolvedAttribute("city"),
      UnresolvedAttribute("listing_age"))

    val filterCondition = And(
      And(
        EqualTo(UnresolvedAttribute("is_owned_by_zillow"), Literal(1)),
        GreaterThanOrEqual(UnresolvedAttribute("bedroom_number"), Literal(3))),
      GreaterThanOrEqual(UnresolvedAttribute("bathroom_number"), Literal(2)))

    val expectedPlan = Project(
      projectList,
      Filter(filterCondition, UnresolvedRelation(TableIdentifier("housing_properties"))))
    // Add to your unit test
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "???")
  }

  ignore("Find which cities in WA state have the largest number of houses for sale") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
      plan(
        pplParser,
        "source = housing_properties | where property_status = 'FOR_SALE' and state = 'WA' | stats count() as count by city | sort -count | head",
        false),
      context)
    // SQL :  SELECT city, COUNT(*) as count FROM housing_properties WHERE property_status = 'FOR_SALE' AND state = 'WA' GROUP BY city ORDER BY count DESC LIMIT 10;
    val aggregateExpressions = Seq(
      Alias(
        AggregateExpression(Count(Literal(1)), mode = Complete, isDistinct = false),
        "count")())
    val groupByAttributes = Seq(UnresolvedAttribute("city"))

    val filterCondition = And(
      EqualTo(UnresolvedAttribute("property_status"), Literal("FOR_SALE")),
      EqualTo(UnresolvedAttribute("state"), Literal("WA")))

    val expectedPlan = Limit(
      Literal(10),
      Sort(
        Seq(SortOrder(UnresolvedAttribute("count"), Descending)),
        true,
        Aggregate(
          groupByAttributes,
          aggregateExpressions,
          Filter(filterCondition, UnresolvedRelation(TableIdentifier("housing_properties"))))))

    // Add to your unit test
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "???")
  }

  ignore("Find the top 5 referrers for the '/' path in apache access logs") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
      plan(pplParser, "source = access_logs | where path = `/` | top 5 referer", false),
      context)
    /*
        SQL: SELECT referer, COUNT(*) as count
        FROM access_logs
              WHERE path = '/' GROUP BY referer ORDER BY count DESC LIMIT 5;
     */
    val aggregateExpressions = Seq(
      Alias(
        AggregateExpression(Count(Literal(1)), mode = Complete, isDistinct = false),
        "count")())
    val groupByAttributes = Seq(UnresolvedAttribute("referer"))
    val filterCondition = EqualTo(UnresolvedAttribute("path"), Literal("/"))
    val expectedPlan = Limit(
      Literal(5),
      Sort(
        Seq(SortOrder(UnresolvedAttribute("count"), Descending)),
        true,
        Aggregate(
          groupByAttributes,
          aggregateExpressions,
          Filter(filterCondition, UnresolvedRelation(TableIdentifier("access_logs"))))))

    // Add to your unit test
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "???")

  }

  ignore(
    "Find access paths by status code. How many error responses (status code 400 or higher) are there for each access path in the Apache access logs") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
      plan(
        pplParser,
        "source = access_logs | where status >= 400 | stats count() by path, status",
        false),
      context)
    /*
        SQL: SELECT path, status, COUNT(*) as count
              FROM access_logs
                WHERE status >=400 GROUP BY path, status;
     */
    val aggregateExpressions = Seq(
      Alias(
        AggregateExpression(Count(Literal(1)), mode = Complete, isDistinct = false),
        "count")())
    val groupByAttributes = Seq(UnresolvedAttribute("path"), UnresolvedAttribute("status"))

    val filterCondition = GreaterThanOrEqual(UnresolvedAttribute("status"), Literal(400))

    val expectedPlan = Aggregate(
      groupByAttributes,
      aggregateExpressions,
      Filter(filterCondition, UnresolvedRelation(TableIdentifier("access_logs"))))

    // Add to your unit test
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "???")
  }

  ignore("Find max size of nginx access requests for every 15min") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
      plan(
        pplParser,
        "source = access_logs | stats max(size)  by span( request_time , 15m) ",
        false),
      context)

    // SQL: SELECT MAX(size) AS max_size, floor(request_time / 900) AS time_span FROM access_logs GROUP BY time_span;
    val aggregateExpressions = Seq(Alias(
      AggregateExpression(Max(UnresolvedAttribute("size")), mode = Complete, isDistinct = false),
      "max_size")())
    val groupByAttributes =
      Seq(Alias(Floor(Divide(UnresolvedAttribute("request_time"), Literal(900))), "time_span")())

    val expectedPlan = Aggregate(
      groupByAttributes,
      aggregateExpressions ++ groupByAttributes,
      UnresolvedRelation(TableIdentifier("access_logs")))

    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "???")

  }

  ignore("Find nginx logs with non 2xx status code and url containing 'products'") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
      plan(
        pplParser,
        "source = sso_logs-nginx-* | where match(http.url, 'products') and http.response.status_code >= `300`",
        false),
      context)
    // SQL : SELECT * FROM `sso_logs-nginx-*` WHERE http.url LIKE '%products%' AND http.response.status_code >= 300;
    val aggregateExpressions = Seq(Alias(
      AggregateExpression(Max(UnresolvedAttribute("size")), mode = Complete, isDistinct = false),
      "max_size")())
    val groupByAttributes =
      Seq(Alias(Floor(Divide(UnresolvedAttribute("request_time"), Literal(900))), "time_span")())

    val expectedPlan = Aggregate(
      groupByAttributes,
      aggregateExpressions,
      UnresolvedRelation(TableIdentifier("access_logs")))

    // Add to your unit test
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "???")

  }

  ignore(
    "Find What are the details (URL, response status code, timestamp, source address) of events in the nginx logs where the response status code is 400 or higher") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
      plan(
        pplParser,
        "source = sso_logs-nginx-* | where http.response.status_code >= `400` | fields http.url, http.response.status_code, @timestamp, communication.source.address",
        false),
      context)
    //    SQL :  SELECT http.url, http.response.status_code, @timestamp, communication.source.address FROM sso_logs-nginx-* WHERE http.response.status_code >= 400;
    val projectList = Seq(
      UnresolvedAttribute("http.url"),
      UnresolvedAttribute("http.response.status_code"),
      UnresolvedAttribute("@timestamp"),
      UnresolvedAttribute("communication.source.address"))

    val filterCondition =
      GreaterThanOrEqual(UnresolvedAttribute("http.response.status_code"), Literal(400))

    val expectedPlan = Project(
      projectList,
      Filter(filterCondition, UnresolvedRelation(TableIdentifier("sso_logs-nginx-*"))))

    // Add to your unit test
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "???")

  }

  ignore(
    "Find What are the average and max http response sizes, grouped by request method, for access events in the nginx logs") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
      plan(
        pplParser,
        "source = sso_logs-nginx-* | where event.name = `access` | stats avg(http.response.bytes), max(http.response.bytes) by http.request.method",
        false),
      context)
    // SQL : SELECT AVG(http.response.bytes) AS avg_size, MAX(http.response.bytes) AS max_size, http.request.method FROM sso_logs-nginx-* WHERE event.name = 'access' GROUP BY http.request.method;
    val aggregateExpressions = Seq(
      Alias(
        AggregateExpression(
          Average(UnresolvedAttribute("http.response.bytes")),
          mode = Complete,
          isDistinct = false),
        "avg_size")(),
      Alias(
        AggregateExpression(
          Max(UnresolvedAttribute("http.response.bytes")),
          mode = Complete,
          isDistinct = false),
        "max_size")())
    val groupByAttributes = Seq(UnresolvedAttribute("http.request.method"))

    val expectedPlan = Aggregate(
      groupByAttributes,
      aggregateExpressions ++ groupByAttributes,
      Filter(
        EqualTo(UnresolvedAttribute("event.name"), Literal("access")),
        UnresolvedRelation(TableIdentifier("sso_logs-nginx-*"))))
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "???")
  }

  ignore(
    "Find flights from which carrier has the longest average delay for flights over 6k miles") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
      plan(
        pplParser,
        "source = opensearch_dashboards_sample_data_flights | where DistanceMiles > 6000 | stats avg(FlightDelayMin) by Carrier | sort -`avg(FlightDelayMin)` | head 1",
        false),
      context)
    // SQL: SELECT AVG(FlightDelayMin) AS avg_delay, Carrier FROM opensearch_dashboards_sample_data_flights WHERE DistanceMiles > 6000 GROUP BY Carrier ORDER BY avg_delay DESC LIMIT 1;
    val aggregateExpressions = Seq(
      Alias(
        AggregateExpression(
          Average(UnresolvedAttribute("FlightDelayMin")),
          mode = Complete,
          isDistinct = false),
        "avg_delay")())
    val groupByAttributes = Seq(UnresolvedAttribute("Carrier"))

    val expectedPlan = Limit(
      Literal(1),
      Sort(
        Seq(SortOrder(UnresolvedAttribute("avg_delay"), Descending)),
        true,
        Aggregate(
          groupByAttributes,
          aggregateExpressions ++ groupByAttributes,
          Filter(
            GreaterThan(UnresolvedAttribute("DistanceMiles"), Literal(6000)),
            UnresolvedRelation(TableIdentifier("opensearch_dashboards_sample_data_flights"))))))

    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "???")

  }

  ignore("Find What's the average ram usage of windows machines over time aggregated by 1 week") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(
      plan(
        pplParser,
        "source = opensearch_dashboards_sample_data_logs | where match(machine.os, 'win') | stats avg(machine.ram) by span(timestamp,1w)",
        false),
      context)
    // SQL : SELECT AVG(machine.ram) AS avg_ram, floor(extract(epoch from timestamp) / 604800)
    //            AS week_span FROM opensearch_dashboards_sample_data_logs WHERE machine.os LIKE '%win%' GROUP BY week_span
    val aggregateExpressions = Seq(
      Alias(
        AggregateExpression(
          Average(UnresolvedAttribute("machine.ram")),
          mode = Complete,
          isDistinct = false),
        "avg_ram")())
    val groupByAttributes = Seq(
      Alias(
        Floor(
          Divide(
            UnixTimestamp(UnresolvedAttribute("timestamp"), Literal("yyyy-MM-dd HH:mm:ss")),
            Literal(604800))),
        "week_span")())

    val expectedPlan = Aggregate(
      groupByAttributes,
      aggregateExpressions ++ groupByAttributes,
      Filter(
        Like(UnresolvedAttribute("machine.os"), Literal("%win%"), '\\'),
        UnresolvedRelation(TableIdentifier("opensearch_dashboards_sample_data_logs"))))

    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "???")

  }
}
