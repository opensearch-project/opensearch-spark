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