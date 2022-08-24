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

import static com.google.cloud.teleport.it.bigtable.BigtableResourceManagerUtils.checkValidTableId;
import static com.google.cloud.teleport.it.bigtable.BigtableResourceManagerUtils.generateDefaultClusters;
import static com.google.cloud.teleport.it.bigtable.BigtableResourceManagerUtils.generateInstanceId;
import static com.google.cloud.teleport.it.common.ResourceManagerUtils.checkValidProjectId;
import static com.google.cloud.teleport.it.common.ResourceManagerUtils.generateNewId;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateInstanceRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default class for implementation of {@link BigtableResourceManager} interface.
 *
 * <p>The class supports one instance, and multiple tables per manager object. An instance is
 * created when the first table is created if one has not been created already.
 *
 * <p>The instance id is formed using testId. The instance id will be "{testId}-{ISO8601 time,
 * microsecond precision}", with additional formatting. Note: If testId is more than 30 characters,
 * a new testId will be formed for naming: {first 21 chars of long testId} + “-” + {8 char hash of
 * testId}.
 *
 * <p>The class is thread-safe.
 */
public class DefaultBigtableResourceManager implements BigtableResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultBigtableResourceManager.class);
  private static final int MAX_TEST_ID_LENGTH = 30;
  private static final String DEFAULT_CLUSTER_ZONE = "us-central1-a";
  private static final int DEFAULT_CLUSTER_NUM_NODES = 1;
  private static final StorageType DEFAULT_CLUSTER_STORAGE_TYPE = StorageType.SSD;

  private final String projectId;
  private final String instanceId;
  private final String testId;
  private final BigtableResourceManagerClientHandler bigtableResourceManagerClientHandler;

  private boolean hasInstance = false;

  @VisibleForTesting
  DefaultBigtableResourceManager(
      String testId, String projectId, CredentialsProvider credentialsProvider) throws IOException {
    this(
        DefaultBigtableResourceManager.builder(testId, projectId)
            .setCredentialsProvider(credentialsProvider));
  }

  @VisibleForTesting
  DefaultBigtableResourceManager(
      String testId,
      String projectId,
      BigtableResourceManagerClientHandler bigtableResourceManagerClientHandler)
      throws IOException {
    this(
        bigtableResourceManagerClientHandler,
        DefaultBigtableResourceManager.builder(testId, projectId));
  }

  private DefaultBigtableResourceManager(DefaultBigtableResourceManager.Builder builder)
      throws IOException {
    this(
        new BigtableResourceManagerClientHandler(
            builder.bigtableInstanceAdminSettings.build(),
            builder.bigtableTableAdminSettings.build(),
            builder.bigtableDataSettings.build()),
        builder.testId,
        builder.projectId,
        builder.instanceId);
  }

  private DefaultBigtableResourceManager(
      BigtableResourceManagerClientHandler bigtableResourceManagerClientHandler,
      DefaultBigtableResourceManager.Builder builder) {
    this(
        bigtableResourceManagerClientHandler,
        builder.testId,
        builder.projectId,
        builder.instanceId);
  }

  private DefaultBigtableResourceManager(
      BigtableResourceManagerClientHandler bigtableResourceManagerClientHandler,
      String testId,
      String projectId,
      String instanceId) {
    this.projectId = projectId;
    this.instanceId = instanceId;
    this.testId = testId;
    this.bigtableResourceManagerClientHandler = bigtableResourceManagerClientHandler;

    // Check that the project ID conforms to GCS standards
    checkValidProjectId(projectId);
  }

  public static DefaultBigtableResourceManager.Builder builder(String testId, String projectId)
      throws IOException {
    return new DefaultBigtableResourceManager.Builder(testId, projectId);
  }

  /**
   * Returns the project ID this Resource Manager is configured to operate on.
   *
   * @return the project ID.
   */
  public String getProjectId() {
    return projectId;
  }

  /**
   * Return the instance ID this Resource Manager uses to create and manage tables in.
   *
   * @return the instance ID.
   */
  public String getInstanceId() {
    return instanceId;
  }

  /**
   * Return the test ID this Resource Manager uses to generate the instance and cluster(s) ID's
   * from.
   *
   * @return test ID.
   */
  public String getTestId() {
    return testId;
  }

  /**
   * Creates a Bigtable instance in which all clusters, nodes and tables will exist.
   *
   * @param cluster A BigtableResourceManagerCluster object to associate with the given Bigtable
   *     instance.
   */
  public synchronized void createInstance(BigtableResourceManagerCluster cluster) {
    createInstance(ImmutableList.of(cluster));
  }

  @Override
  public synchronized void createInstance(Iterable<BigtableResourceManagerCluster> clusters) {

    // Check to see if instance already exists, and throw error if it does
    if (hasInstance) {
      throw new IllegalStateException(
          "Instance " + instanceId + " already exists for project " + projectId + ".");
    }

    LOG.info("Creating instance {} in project {}.", instanceId, projectId);

    // Create instance request object and add all the given clusters to the request
    CreateInstanceRequest request = CreateInstanceRequest.of(instanceId);
    for (BigtableResourceManagerCluster cluster : clusters) {
      request.addCluster(
          cluster.getClusterId(),
          cluster.getZone(),
          cluster.getNumNodes(),
          cluster.getStorageType());
    }

    // Send the instance request to Google Cloud
    try {
      bigtableResourceManagerClientHandler.getBigtableInstanceAdminClient().createInstance(request);
    } catch (Exception e) {
      throw new BigtableResourceManagerException("Failed to create instance.", e);
    }
    hasInstance = true;

    LOG.info("Successfully created instance {}.", instanceId);
  }

  /**
   * Helper method for determining if an instance has been created for the ResourceManager object.
   *
   * @throws IllegalStateException if an instance has not yet been created.
   */
  private void checkHasInstance() {
    if (!hasInstance) {
      throw new IllegalStateException("There is no instance for manager to perform operation on.");
    }
  }

  /**
   * Helper method for determining if the given tableId exists in the instance.
   *
   * @param tableId The id of the table to check.
   * @throws IllegalStateException if the table does not exist in the instance.
   */
  private void checkHasTable(String tableId) {
    if (!bigtableResourceManagerClientHandler.getBigtableTableAdminClient().exists(tableId)) {
      throw new IllegalStateException(
          "The table " + tableId + " does not exist in instance " + instanceId + ".");
    }
  }

  @Override
  public synchronized void createTable(String tableId, Iterable<String> columnFamilies) {
    // Check table ID
    checkValidTableId(tableId);

    // Check for at least one column family
    if (!columnFamilies.iterator().hasNext()) {
      throw new IllegalArgumentException(
          "There must be at least one column family specified when creating a table.");
    }

    // Create a default instance if this resource manager has not already created one
    if (!hasInstance) {
      createInstance(
          generateDefaultClusters(
              instanceId,
              DEFAULT_CLUSTER_ZONE,
              DEFAULT_CLUSTER_NUM_NODES,
              DEFAULT_CLUSTER_STORAGE_TYPE));
    }
    checkHasInstance();

    LOG.info("Creating table using tableId '{}'.", tableId);

    // Fetch the Bigtable Table client and create the table if it does not already exist in the
    // instance
    try {
      if (!bigtableResourceManagerClientHandler.getBigtableTableAdminClient().exists(tableId)) {
        CreateTableRequest createTableRequest = CreateTableRequest.of(tableId);
        for (String columnFamily : columnFamilies) {
          createTableRequest.addFamily(columnFamily);
        }
        bigtableResourceManagerClientHandler
            .getBigtableTableAdminClient()
            .createTable(createTableRequest);

      } else {
        LOG.info("Skipping table creation as table {}.{} already exists.", instanceId, tableId);
      }
    } catch (Exception e) {
      throw new BigtableResourceManagerException("Failed to create table.", e);
    }

    LOG.info("Successfully created table {}.{}", instanceId, tableId);
  }

  @Override
  public synchronized void write(RowMutation tableRow) {
    write(ImmutableList.of(tableRow));
  }

  @Override
  public synchronized void write(Iterable<RowMutation> tableRows) {
    checkHasInstance();

    // Exit early if there are no mutations
    if (!tableRows.iterator().hasNext()) {
      return;
    }

    LOG.info("Sending {} mutations to instance {}.", Iterables.size(tableRows), instanceId);

    // Fetch the Bigtable data client and send row mutations to the table
    try (BigtableDataClient dataClient =
        bigtableResourceManagerClientHandler.getBigtableDataClient()) {
      for (RowMutation tableRow : tableRows) {
        dataClient.mutateRow(tableRow);
      }
    } catch (Exception e) {
      throw new BigtableResourceManagerException("Failed to write mutations.", e);
    }

    LOG.info("Successfully sent mutations to instance {}.", instanceId);
  }

  @Override
  public synchronized ImmutableList<Row> readTable(String tableId) {
    checkHasInstance();
    checkHasTable(tableId);

    // List to store fetched rows
    ImmutableList.Builder<Row> tableRowsBuilder = ImmutableList.builder();

    LOG.info("Reading all rows from {}.", tableId);

    // Fetch the Bigtable data client and read all the rows from the table given by tableId
    try (BigtableDataClient dataClient =
        bigtableResourceManagerClientHandler.getBigtableDataClient()) {

      Query query = Query.create(tableId);
      ServerStream<Row> rowStream = dataClient.readRows(query);
      for (Row row : rowStream) {
        tableRowsBuilder.add(row);
      }

    } catch (IOException | NotFoundException e) {
      throw new BigtableResourceManagerException("Error occurred while reading table rows.", e);
    }

    ImmutableList<Row> tableRows = tableRowsBuilder.build();
    LOG.info("Loaded {} rows from {}.", tableRows.size(), tableId);

    return tableRows;
  }

  @Override
  public synchronized void cleanupAll() {
    LOG.info("Attempting to cleanup manager.");
    try {
      bigtableResourceManagerClientHandler
          .getBigtableInstanceAdminClient()
          .deleteInstance(instanceId);
      bigtableResourceManagerClientHandler.getBigtableInstanceAdminClient().close();
      bigtableResourceManagerClientHandler.getBigtableTableAdminClient().close();
      hasInstance = false;
    } catch (Exception e) {
      throw new BigtableResourceManagerException("Failed to delete resources.", e);
    }

    LOG.info("Manager successfully cleaned up.");
  }

  /** Builder for {@link DefaultBigtableResourceManager}. */
  public static final class Builder {

    private final String testId;
    private final String projectId;
    private String instanceId;
    private CredentialsProvider credentialsProvider;
    private final BigtableInstanceAdminSettings.Builder bigtableInstanceAdminSettings;
    private final BigtableTableAdminSettings.Builder bigtableTableAdminSettings;
    private final BigtableDataSettings.Builder bigtableDataSettings;

    private Builder(String testId, String projectId) {

      // Generate and format ID's
      if (testId.length() > MAX_TEST_ID_LENGTH) {
        testId = generateNewId(testId, MAX_TEST_ID_LENGTH);
      }
      this.testId = testId;
      this.projectId = projectId;
      this.instanceId = generateInstanceId(testId);

      this.bigtableInstanceAdminSettings =
          BigtableInstanceAdminSettings.newBuilder().setProjectId(projectId);
      this.bigtableTableAdminSettings =
          BigtableTableAdminSettings.newBuilder().setProjectId(projectId).setInstanceId(instanceId);
      this.bigtableDataSettings =
          BigtableDataSettings.newBuilder().setProjectId(projectId).setInstanceId(instanceId);
    }

    public String getTestId() {
      return testId;
    }

    public String getProjectId() {
      return projectId;
    }

    public Builder setInstanceId(String instanceId) {
      this.instanceId = instanceId;
      return this;
    }

    public String getInstanceId() {
      return instanceId;
    }

    public Builder setCredentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    public CredentialsProvider getCredentialsProvider() {
      return credentialsProvider;
    }

    public BigtableInstanceAdminSettings getBigtableInstanceAdminSettings() throws IOException {
      return bigtableInstanceAdminSettings.build();
    }

    public BigtableTableAdminSettings getBigtableTableAdminSettings() throws IOException {
      return bigtableTableAdminSettings.build();
    }

    public BigtableDataSettings getBigtableDataSettings() {
      return bigtableDataSettings.build();
    }

    public DefaultBigtableResourceManager build() throws IOException {
      return new DefaultBigtableResourceManager(this);
    }
  }
}
