/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.bigtable;

import com.google.cloud.bigtable.admin.v2.models.StorageType;
import java.io.IOException;

/**
 * Class for storing the metadata of a Bigtable cluster object.
 *
 * <p>A cluster belongs to a single bigtable instance and represents the service in a given zone. A
 * cluster can have multiple nodes operating on the data. The cluster also has a storage type of
 * either SSD or HDD depending on the user's needs.
 */
public class BigtableResourceManagerCluster {

  private final String clusterId;

  private final String zone;

  private final int numNodes;

  private final StorageType storageType;

  BigtableResourceManagerCluster(BigtableResourceManagerCluster.Builder builder) {
    this(builder.clusterId, builder.zone, builder.numNodes, builder.storageType);
  }

  BigtableResourceManagerCluster(
      String clusterId, String zone, int numNodes, StorageType storageType) {
    this.clusterId = clusterId;
    this.zone = zone;
    this.numNodes = numNodes;
    this.storageType = storageType;
  }

  /**
   * Returns the cluster ID of the Bigtable cluster object.
   *
   * @return the ID of the Bigtable cluster.
   */
  public String getClusterId() {
    return clusterId;
  }

  /**
   * Returns the operating zone of the Bigtable cluster object.
   *
   * @return the zone of the Bigtable cluster.
   */
  public String getZone() {
    return zone;
  }

  /**
   * Returns the number of nodes the Bigtable cluster object should be configured with.
   *
   * @return the number of nodes for the Bigtable cluster.
   */
  public int getNumNodes() {
    return numNodes;
  }

  /**
   * Returns the type of storage the Bigtable cluster object should be configured with (SSD or HDD).
   *
   * @return the storage type of the Bigtable cluster.
   */
  public StorageType getStorageType() {
    return storageType;
  }

  public static BigtableResourceManagerCluster.Builder builder(
      String clusterId, String zone, int numNodes, StorageType storageType) {
    return new BigtableResourceManagerCluster.Builder(clusterId, zone, numNodes, storageType);
  }

  /** Builder for {@link BigtableResourceManagerCluster}. */
  public static final class Builder {

    private final String clusterId;

    private final String zone;

    private final int numNodes;

    private final StorageType storageType;

    private Builder(String clusterId, String zone, int numNodes, StorageType storageType) {
      this.clusterId = clusterId;
      this.zone = zone;
      this.numNodes = numNodes;
      this.storageType = storageType;
    }

    public BigtableResourceManagerCluster build() throws IOException {
      return new BigtableResourceManagerCluster(this);
    }
  }
}
