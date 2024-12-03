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

| Column Name    | Description                                                        |
|----------------|--------------------------------------------------------------------|
| ip_range_start | An integer value used to determine if an IP address is in a subnet |
| ip_range_end   | An integer value used to determine if an IP address is in a subnet |
| ipv4           | A boolean value, `true` if the IP address subnet is in IPv4 format |

## Run the Script

1. Create a copy of the scala file `load_geoip_data.scala`
2. Edit the copy of the file `load_geoip_data.scala`
   There are three variables that need to be updated.
   1. `FILE_PATH_TO_INPUT_CSV` - the full path to the CSV file to load
   2. `FILE_PATH_TO_OUTPUT_CSV` - the full path of the CSV file to write the sanitized data to
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
**Run the Script**, but make sure that `TABLE_NAME` is set to `null`. Upload the
`FILE_PATH_TO_OUTPUT_CSV` to S3.

## End-to-End

How to download a sample data GeoIP location data set, clean it up and import it into a
Spark table.

1. Use a web browser to download the [data set Zip file](https://geoip.maps.opensearch.org/v1/geolite2-city/data/geolite2-city_1732905911000.zip)
2. Extract the Zip file
3. Copy the file `geolite2-City.csv` to the computer where you run `spark-shell`
4. Copy the file file `load_geoip_data.scala` to the computer where you run `spark-shell`
5. Connect to the computer where you run `spark-shell`
6. Change to the directory containing `geolite2-City.csv` and `load_geoip_data.scala`
7. Update the `load_geoip_data.scala` file to specify the CSV files to read and write. Also update
   it to specify the Spark table to create (`geo_ip_data` in this case).
   ```
   sed -i \
       -e "s#^var FILE_PATH_TO_INPUT_CSV: String =.*#var FILE_PATH_TO_INPUT_CSV: String = \"${PWD}/geolite2-City.csv\"#" \
       load_geoip_data.scala
   sed -i \
       -e "s#^var FILE_PATH_TO_OUTPUT_CSV: String = .*#var FILE_PATH_TO_OUTPUT_CSV: String = \"${PWD}/geolite2-City-fixed.csv\"#" \
       load_geoip_data.scala
   sed -i \
       -e 's#^var TABLE_NAME: String = .*#var TABLE_NAME: String = "geo_ip_data"#' \
       load_geoip_data.scala
   ```
8. Run `spark-shell`
   ```
   spark-shell
   ```
9. Load and run the `load_geoip_data.scala` script
   ```
   :load load_geoip_data.scala
   ```
