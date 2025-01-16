/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazonaws.services.emrserverless;

import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.client.AwsSyncClientParams;
import com.amazonaws.client.builder.AwsSyncClientBuilder;

import org.opensearch.spark.emrserverless.DockerEMRServerlessClient;

public class AWSEMRServerlessClientBuilder extends AwsSyncClientBuilder<AWSEMRServerlessClientBuilder, AWSEMRServerless> {
  private static final ClientConfigurationFactory CLIENT_CONFIG_FACTORY = new ClientConfigurationFactory();

  private AWSEMRServerlessClientBuilder() {
    super(CLIENT_CONFIG_FACTORY);
  }

  public static AWSEMRServerlessClientBuilder standard() {
    return new AWSEMRServerlessClientBuilder();
  }

  public static AWSEMRServerless defaultClient() {
    return (AWSEMRServerless) standard().build();
  }

  protected AWSEMRServerless build(AwsSyncClientParams params) {
    DockerEMRServerlessClient client = new DockerEMRServerlessClient(CLIENT_CONFIG_FACTORY.getConfig());
    client.setServiceNameIntern("emr");
    return client;
  }
}
