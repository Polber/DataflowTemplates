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

import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;

/**
 * Class for storing the 3 Bigtable clients used to manage the Instance(s), Table(s) and Data
 * mutations performed by the resource manager.
 *
 * <p>This class is intended to simplify mock injections of Bigtable clients during unit testing
 */
public class BigtableResourceManagerClientHandler {

  private final BigtableInstanceAdminClient bigtableInstanceAdminClient;
  private final BigtableTableAdminClient bigtableTableAdminClient;
  private final BigtableDataSettings bigtableDataSettings;

  private BigtableResourceManagerClientHandler(BigtableResourceManagerClientHandler.Builder builder)
      throws IOException {
    this(builder.instanceSettings, builder.tableSettings, builder.dataSettings);
  }

  @VisibleForTesting
  BigtableResourceManagerClientHandler(
      BigtableInstanceAdminSettings instanceSettings,
      BigtableTableAdminSettings tableSettings,
      BigtableDataSettings dataSettings)
      throws IOException {
    this.bigtableInstanceAdminClient = BigtableInstanceAdminClient.create(instanceSettings);
    this.bigtableTableAdminClient = BigtableTableAdminClient.create(tableSettings);
    this.bigtableDataSettings = dataSettings;
  }

  public static BigtableResourceManagerClientHandler.Builder builder(
      BigtableInstanceAdminSettings instanceSettings,
      BigtableTableAdminSettings tableSettings,
      BigtableDataSettings dataSettings) {
    return new BigtableResourceManagerClientHandler.Builder(
        instanceSettings, tableSettings, dataSettings);
  }

  /**
   * Returns the Instance admin client for managing Bigtable instances.
   *
   * @return the instance admin client
   */
  public BigtableInstanceAdminClient getBigtableInstanceAdminClient() {
    return bigtableInstanceAdminClient;
  }

  /**
   * Returns the Table admin client for managing Bigtable tables.
   *
   * @return the table admin client
   */
  public BigtableTableAdminClient getBigtableTableAdminClient() {
    return bigtableTableAdminClient;
  }

  /**
   * Returns the Data client for reading and writing data to Bigtable tables.
   *
   * @return the data client
   */
  public BigtableDataClient getBigtableDataClient() throws IOException {
    return BigtableDataClient.create(bigtableDataSettings);
  }

  /** Builder for {@link BigtableResourceManagerClientHandler}. */
  public static final class Builder {

    private final BigtableInstanceAdminSettings instanceSettings;
    private final BigtableTableAdminSettings tableSettings;
    private final BigtableDataSettings dataSettings;

    private Builder(
        BigtableInstanceAdminSettings instanceSettings,
        BigtableTableAdminSettings tableSettings,
        BigtableDataSettings dataSettings) {
      this.instanceSettings = instanceSettings;
      this.tableSettings = tableSettings;
      this.dataSettings = dataSettings;
    }

    public BigtableResourceManagerClientHandler build() throws IOException {
      return new BigtableResourceManagerClientHandler(this);
    }
  }
}
