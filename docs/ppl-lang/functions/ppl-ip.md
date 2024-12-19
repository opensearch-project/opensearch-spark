## PPL IP Address Functions

### `CIDRMATCH`

**Description**

`CIDRMATCH(ip, cidr)` checks if ip is within the specified cidr range.

**Argument type:**
 - STRING, STRING
 - Return type: **BOOLEAN**

Example:

    os> source=ips | where cidrmatch(ip, '192.169.1.0/24') | fields ip
    fetched rows / total rows = 1/1
    +--------------+
    | ip           |
    |--------------|
    | 192.169.1.5  |
    +--------------+

    os> source=ipsv6 | where cidrmatch(ip, '2003:db8::/32') | fields ip
    fetched rows / total rows = 1/1
    +-----------------------------------------+
    | ip                                      |
    |-----------------------------------------|
    | 2003:0db8:0000:0000:0000:0000:0000:0000 |
    +-----------------------------------------+

Note:
 - `ip` can be an IPv4 or an IPv6 address
 - `cidr` can be an IPv4 or an IPv6 block
 - `ip` and `cidr` must be either both IPv4 or both IPv6
 - `ip` and `cidr` must both be valid and non-empty/non-null

### `GEOIP`

**Description**

`GEOIP(ip[, property]...)` retrieves geospatial data corresponding to the provided `ip`.

**Argument type:**
- `ip` is string be **STRING** representing an IPv4 or an IPv6 address.
- `property` is **STRING** and must be one of the following:
    - `COUNTRY_ISO_CODE`
    - `COUNTRY_NAME`
    - `CONTINENT_NAME`
    - `REGION_ISO_CODE`
    - `REGION_NAME`
    - `CITY_NAME`
    - `TIME_ZONE`
    - `LOCATION`
- Return type:
    - **STRING** if one property given
    - **STRUCT_TYPE** if more than one or no property is given

Example:

_Without properties:_

    os> source=ips | eval a = geoip(ip) | fields ip, a
    fetched rows / total rows = 2/2
    +---------------------+-------------------------------------------------------------------------------------------------------+
    |ip                   |lol                                                                                                    |
    +---------------------+-------------------------------------------------------------------------------------------------------+
    |66.249.157.90        |{JM, Jamaica, North America, 14, Saint Catherine Parish, Portmore, America/Jamaica, 17.9686,-76.8827}  |
    |2a09:bac2:19f8:2ac3::|{CA, Canada, North America, PE, Prince Edward Island, Charlottetown, America/Halifax, 46.2396,-63.1355}|
    +---------------------+-------+------+-------------------------------------------------------------------------------------------------------+

_With one property:_

    os> source=users | eval a = geoip(ip, COUNTRY_NAME) | fields ip, a
    fetched rows / total rows = 2/2
    +---------------------+-------+
    |ip                   |a      |
    +---------------------+-------+
    |66.249.157.90        |Jamaica|
    |2a09:bac2:19f8:2ac3::|Canada |
    +---------------------+-------+

_With multiple properties:_

    os> source=users | eval a = geoip(ip, COUNTRY_NAME, REGION_NAME, CITY_NAME) | fields ip, a
    fetched rows / total rows = 2/2
    +---------------------+---------------------------------------------+
    |ip                   |a                                            |
    +---------------------+---------------------------------------------+
    |66.249.157.90        |{Jamaica, Saint Catherine Parish, Portmore}  |
    |2a09:bac2:19f8:2ac3::|{Canada, Prince Edward Island, Charlottetown}|
    +---------------------+---------------------------------------------+

Note:
- To use `geoip` user must create spark table containing geo ip location data. Instructions to create table can be found [here](../../opensearch-geoip.md).
    - `geoip` command by default expects the created table to be called `geoip_ip_data`.
    - if a different table name is desired, can set `spark.geoip.tablename` spark config to new table name.
- `ip` can be an IPv4 or an IPv6 address.
- `geoip` commands will always calculated first if used with other eval functions.
