## geoip syntax proposal

geoip function to add information about the geographical location of an IPv4 or IPv6 address

1. **Proposed syntax**
    - `... | eval geoinfo = geoip([datasource,] ipAddress [,properties])`
        - generic syntax     
    - `... | eval geoinfo = geoip(ipAddress)`
        - use the default geoip datasource
    - `... | eval geoinfo = geoip("abc", ipAddress)`
        - use the "abc" geoip datasource
    - `... | eval geoinfo = geoip(ipAddress, "city,lat,lon")`
        -  use the default geoip datasource, retrieve only city, lat and lon
    - `... | eval geoinfo = geoip("abc", ipAddress, "city,lat,lon")`
        - use the "abc" geoip datasource, retrieve only city, lat and lon


2. **Proposed wiring with the geoip database**
    - Leverage the functionality of the ip2geo processor
      - ip2geo processor configuration, functionality and code will be used
      - Prerequisite for the geoip is that ip2geo processor is configured properly
      - See https://opensearch.org/docs/latest/ingest-pipelines/processors/ip2geo/


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

