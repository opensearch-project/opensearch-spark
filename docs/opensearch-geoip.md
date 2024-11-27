# OpenSearch Geographic IP Location Data

## Overview

OpenSearch has PPL functions for looking up the geographic location of IP addresses. In order
to use these functions, a table needs to be created containing the geographic location
information.

## How to Create Geographic Location Index

A script has been created that can cleanup and augment a CSV file that contains geographic
location information for IP addresses ranges. The CSV file is expected to have the following
columns:

| Column Name      | Description                                                                                             |
|------------------|---------------------------------------------------------------------------------------------------------|
| cidr             | IP address subnet in format `IP_ADDRESS/NETMASK` (ex. `192.168.0.0/24`). IP address can be IPv4 or IPv6 |
| country_iso_code | ISO code of the country where the IP address subnet is located                                          |
| country_name     | Name of the country where the IP address subnet is located                                              |
| continent_name   | Name of the continent where the IP address subent is located                                            |
| region_iso_code  | ISO code of the region where the IP address subnet is located                                           |
| region_name      | Name of the region where the IP address subnet is located                                               |
| city_name        | Name of the city where the IP address subnet is located                                                 |
| time_zone        | Time zone where the IP address subnet is located                                                        |
| location         | Latitude and longitude where the IP address subnet is located                                           |

The script will cleanup the data by splitting IP address subnets so that an IP address can only be in at most one subnet.

The data is augmented by adding 3 fields.

| Column Name | Description                                                        |
|-------------|--------------------------------------------------------------------|
| start       | An integer value used to determine if an IP address is in a subnet |
| end         | An integer value used to determine if an IP address is in a subnet |
| ipv4        | A boolean value, `true` if the IP address subnet is in IPv4 format |

## Run the Script

1. Create a copy of the scala file `load_geoip_data.scala`
2. Edit the file
3. There are three variables that need to be updated.
   1. `INPUT_FILE` - the full path to the CSV file to load
   2. `OUTPUT_FILE` - the full path of the CSV file to write the sanitized data to
   3. `TABLE_NAME` - name of the index to create in OpenSearch. No table is created if this is null
4. Save the file
5. Run the Apache Spark CLI and connect to the database
6. Load the Scala script
   ```scala
   :load FILENAME
   ```
   Replace `FILENAME` with the full path to the Scala script.

## Notes for EMR

With EMR it is necessary to load the data from an S3 object. Follow the instructions for
**Run the Script**, but make sure that `TABLE_NAME` is set to `null`. Upload the `OUTPUT_FILE`
to S3.