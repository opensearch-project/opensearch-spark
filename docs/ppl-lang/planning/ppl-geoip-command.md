## geoip syntax proposal

geoip function to add information about the geographical location of an IPv4 or IPv6 address

**Implementation syntax**
- `... | eval geoinfo = geoip(ipAddress *[,properties])`
- generic syntax     
- `... | eval geoinfo = geoip(ipAddress)`
- retrieves all geo data
- `... | eval geoinfo = geoip(ipAddress, city, location)`
-  retrieve only city, and location

**Implementation details**
- Current implementation requires user to have created a geoip table. Geoip table has the following schema:

    ```SQL
        CREATE TABLE geoip (
            cidr STRING,
            country_iso_code STRING,
            country_name STRING,
            continent_name STRING,
            region_iso_code STRING,
            region_name STRING,
            city_name STRING,
            time_zone STRING,
            location STRING,
            ip_range_start BIGINT,
            ip_range_end BIGINT,
            ipv4 BOOLEAN
        )
    ```     

- `geoip` is resolved by performing a join on said table and projecting the resulting geoip data as a struct.
- an example of using `geoip` is equivalent to running the following SQL query:

   ```SQL
        SELECT source.*, struct(geoip.country_name, geoip.city_name) AS a
        FROM source, geoip
        WHERE geoip.ip_range_start <= ip_to_int(source.ip)
          AND geoip.ip_range_end > ip_to_int(source.ip)
          AND geoip.ip_type = is_ipv4(source.ip);
   ```
- in the case that only one property is provided in function call, `geoip` returns string of specified property instead: 
  
  ```SQL
        SELECT source.*, geoip.country_name AS a
        FROM source, geoip
        WHERE geoip.ip_range_start <= ip_to_int(source.ip)
          AND geoip.ip_range_end > ip_to_int(source.ip)
          AND geoip.ip_type = is_ipv4(source.ip);
  ```

**Future plan for additional data-sources**

- Currently only using pre-existing geoip table defined within spark is possible.
- There is future plans to allow users to specify data sources:
    - API data sources - if users have their own geoip provided will create ability for users to configure and call said endpoints
    - OpenSearch geospatial client - once geospatial client is published we can leverage client to utilize OpenSearch geo2ip functionality.
- Additional datasource connection params will be provided through spark config options.

### New syntax definition in ANTLR

```ANTLR
  
// functions
evalFunctionCall
   : evalFunctionName LT_PRTHS functionArgs RT_PRTHS
   | geoipFunction
   ;  
  
geoipFunction
   : GEOIP LT_PRTHS (datasource = functionArg COMMA)? ipAddress = functionArg (COMMA properties = stringLiteral)? RT_PRTHS
   ;
```
