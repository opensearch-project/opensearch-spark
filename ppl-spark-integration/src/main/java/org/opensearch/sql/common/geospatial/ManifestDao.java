/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.geospatial;

import static org.opensearch.sql.common.geospatial.DatasourceDao.createCidrBitSet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.BitSet;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.Pair;

public class ManifestDao implements DatasourceDao {

  /**
   * Default endpoint to be used in GeoIP datasource creation API
   */
  // TODO: Make this a configurable setting.
  public static final String DATASOURCE_ENDPOINT =
      "https://geoip.maps.opensearch.org/v1/geolite2-city/manifest.json";

  private final DatasourceManifest manifest;
  private CSVParser manifestCsv;

  public ManifestDao() throws MalformedURLException {
    manifest = DatasourceManifest.Builder.build(new URL(DATASOURCE_ENDPOINT));
  }

  @Override
  public Stream<Pair<BitSet, GeoIpData>> getGeoIps() {
    manifestCsv = getDatabaseReader(manifest);

    Map<String, Integer> headerMap = manifestCsv.getHeaderMap();
    int cidr_index             = headerMap.get("cidr");
    int country_iso_code_index = headerMap.get("country_iso_code");
    int country_name_index     = headerMap.get("country_name");
    int continent_name_index   = headerMap.get("continent_name");
    int region_iso_code_index  = headerMap.get("region_iso_code");
    int region_name_index      = headerMap.get("region_name");
    int city_name_index        = headerMap.get("city_name");
    int time_zone_index        = headerMap.get("time_zone");
    int lat_index              = headerMap.get("lat");
    int lon_index              = headerMap.get("lon");

    return StreamSupport.stream(manifestCsv.spliterator(), false)
        .map(record -> {
          return Pair.of(
              createCidrBitSet(record.get(cidr_index)),
              GeoIpData.builder()
                .country_iso_code(record.get(country_iso_code_index))
                .country_name(record.get(country_name_index))
                .continent_name(record.get(continent_name_index))
                .region_iso_code(record.get(region_iso_code_index))
                .region_name(record.get(region_name_index))
                .city_name(record.get(city_name_index))
                .time_zone(record.get(time_zone_index))
                .lat(record.get(lat_index))
                .lon(record.get(lon_index))
                .build());
        });
  }

  @Override
  public void close() throws Exception {
    if (manifestCsv != null) {
      manifestCsv.close();
      manifestCsv = null;
    }
  }

  /**
   * Create CSVParser of a GeoIP data
   *
   * @param manifest Datasource manifest
   * @return CSVParser for GeoIP data
   */
  public CSVParser getDatabaseReader(final DatasourceManifest manifest) {
    try {
      URL zipUrl = new URL(manifest.getUrl());
      return internalGetDatabaseReader(manifest, zipUrl.openConnection());
    } catch (IOException e) {
      throw new RuntimeException(String.format("failed to read geoip data from %s", manifest.getUrl()), e);
    }
  }

  protected CSVParser internalGetDatabaseReader(final DatasourceManifest manifest, final URLConnection connection) throws IOException {
  //  connection.addRequestProperty(Constants.USER_AGENT_KEY, Constants.USER_AGENT_VALUE);
    ZipInputStream zipIn = new ZipInputStream(connection.getInputStream());
    ZipEntry zipEntry = zipIn.getNextEntry();
    while (zipEntry != null) {
      if (zipEntry.getName().equalsIgnoreCase(manifest.getDbName()) == false) {
        zipEntry = zipIn.getNextEntry();
        continue;
      }
      return new CSVParser(new BufferedReader(new InputStreamReader(zipIn)), CSVFormat.RFC4180);
    }
    throw new RuntimeException(String.format("database file [%s] does not exist in the zip file [%s]", manifest.getDbName(), manifest.getUrl()));
  }
}
