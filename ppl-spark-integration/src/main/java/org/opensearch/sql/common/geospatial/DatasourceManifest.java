/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.geospatial;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Ip2Geo datasource manifest file object
 *
 * Manifest file is stored in an external endpoint. OpenSearch read the file and store values it in this object.
 */
@Setter
@Getter
@AllArgsConstructor
public class DatasourceManifest {
  /**
   * @param url URL of a ZIP file conFtaining a database
   * @return URL of a ZIP file containing a database
   */
  public String url;
  /**
   * @param dbName A database file name inside the ZIP file
   * @return A database file name inside the ZIP file
   */
  public String dbName;
  /**
   * @param sha256Hash SHA256 hash value of a database file
   * @return SHA256 hash value of a database file
   */
  @JsonProperty("sha256_hash")
  public String sha256Hash;
  /**
   * @param validForInDays A duration in which the database file is valid to use
   * @return A duration in which the database file is valid to use
   */
  @JsonProperty("valid_for_in_days")
  public Long validForInDays;
  /**
   * @param updatedAt A date when the database was updated
   * @return A date when the database was updated
   */
  @JsonProperty("updated_at_in_epoch_milli")
  public Long updatedAt;
  /**
   * @param provider A database provider name
   * @return A database provider name
   */
  public String provider;

  /**
   * Datasource manifest builder
   */
  public static class Builder {
    private static final int MANIFEST_FILE_MAX_BYTES = 1024 * 8;

    /**
     * Build DatasourceManifest from a given url
     *
     * @param url url to downloads a manifest file
     * @return DatasourceManifest representing the manifest file
     */
    public static DatasourceManifest build(final URL url) {
      try {
        URLConnection connection = url.openConnection();
        return internalBuild(connection);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    protected static DatasourceManifest internalBuild(final URLConnection connection) throws IOException {
//      connection.addRequestProperty(Constants.USER_AGENT_KEY, Constants.USER_AGENT_VALUE);
      final ObjectMapper mapper = new ObjectMapper();
      InputStreamReader inputStreamReader = new InputStreamReader(connection.getInputStream());
      try (BufferedReader reader = new BufferedReader(inputStreamReader)) {
        return mapper.readValue(reader, DatasourceManifest.class);
      }
    }
  }
}
