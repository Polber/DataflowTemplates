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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link com.google.cloud.teleport.it.bigtable.DefaultBigtableResourceManager}. */
@RunWith(JUnit4.class)
public class DefaultBigtableResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ServerStream<Row> readRows;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private BigtableResourceManagerClientHandler bigtableResourceManagerClientHandler;

  @Mock private BigtableInstanceAdminClient bigtableInstanceAdminClient;
  @Mock private BigtableTableAdminClient bigtableTableAdminClient;
  @Mock private BigtableDataClient bigtableDataClient;

  @Mock private CredentialsProvider credentialsProvider;

  private static final String TEST_ID = "test-id";
  private static final String TABLE_ID = "table-id";
  private static final String PROJECT_ID = "test-project";

  private static final String CLUSTER_ID = "cluster-id";
  private static final String CLUSTER_ZONE = "us-central1-a";
  private static final int CLUSTER_NUM_NODES = 1;
  private static final StorageType CLUSTER_STORAGE_TYPE = StorageType.SSD;

  private DefaultBigtableResourceManager testManager;
  private Iterable<BigtableResourceManagerCluster> cluster;

  @Before
  public void setUp() throws IOException {
    testManager =
        new DefaultBigtableResourceManager(
            TEST_ID, PROJECT_ID, bigtableResourceManagerClientHandler, credentialsProvider);
    cluster =
        ImmutableList.of(
            new BigtableResourceManagerCluster(
                CLUSTER_ID, CLUSTER_ZONE, CLUSTER_NUM_NODES, CLUSTER_STORAGE_TYPE));
  }

  private void prepareCreateInstanceAdminClientMock() {
    when(bigtableResourceManagerClientHandler.getBigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
  }

  private void prepareCreateTableAdminClientMock() {
    when(bigtableResourceManagerClientHandler.getBigtableTableAdminClient())
        .thenReturn(bigtableTableAdminClient);
  }

  private void prepareCreateDataClientMock() throws IOException {
    when(bigtableResourceManagerClientHandler.getBigtableDataClient())
        .thenReturn(bigtableDataClient);
  }

  @Test
  public void testCreateResourceManagerCreatesCorrectIdValues() {
    assertThat(testManager.getInstanceId()).matches(TEST_ID + "-\\d{8}-\\d{6}-\\d{6}");
    assertThat(testManager.getProjectId()).matches(PROJECT_ID);
    assertThat(testManager.getTestId()).matches(TEST_ID);
  }

  @Test
  public void testCreateInstanceShouldThrowExceptionWhenInstanceAlreadyExists() {
    prepareCreateInstanceAdminClientMock();

    testManager.createInstance(cluster);

    assertThrows(IllegalStateException.class, () -> testManager.createInstance(cluster));
  }

  @Test
  public void testCreateInstanceShouldWorkWhenBigtableDoesNotThrowAnyError() {
    prepareCreateInstanceAdminClientMock();

    when(bigtableInstanceAdminClient.createInstance(any())).thenThrow(IllegalStateException.class);

    assertThrows(IllegalStateException.class, () -> testManager.createInstance(cluster));
  }

  @Test
  public void testCreateTableShouldNotCreateInstanceWhenInstanceAlreadyExists() {
    prepareCreateInstanceAdminClientMock();
    prepareCreateTableAdminClientMock();

    testManager.createInstance(cluster);
    Mockito.lenient()
        .when(bigtableInstanceAdminClient.createInstance(any()))
        .thenThrow(IllegalStateException.class);

    when(bigtableTableAdminClient.exists(anyString())).thenReturn(false);
    testManager.createTable(TABLE_ID, ImmutableList.of("cf1"));
  }

  @Test
  public void testCreateTableShouldCreateInstanceWhenInstanceDoesNotExist() {
    prepareCreateInstanceAdminClientMock();
    prepareCreateTableAdminClientMock();

    when(bigtableInstanceAdminClient.createInstance(any())).thenThrow(IllegalStateException.class);

    assertThrows(
        IllegalStateException.class,
        () -> testManager.createTable(TABLE_ID, ImmutableList.of("cf1")));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenNoColumnFamilyGiven() {
    prepareCreateInstanceAdminClientMock();
    prepareCreateTableAdminClientMock();

    assertThrows(
        IllegalArgumentException.class, () -> testManager.createTable(TABLE_ID, new ArrayList<>()));
  }

  @Test
  public void testCreateTableShouldNotCreateTableWhenTableAlreadyExists() {
    prepareCreateInstanceAdminClientMock();
    prepareCreateTableAdminClientMock();

    when(bigtableTableAdminClient.exists(anyString())).thenReturn(true);
    Mockito.lenient()
        .when(bigtableTableAdminClient.createTable(any()))
        .thenThrow(RuntimeException.class);

    testManager.createTable(TABLE_ID, ImmutableList.of("cf1"));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenTableAdminClientFailsToCreateTable() {
    prepareCreateInstanceAdminClientMock();
    prepareCreateTableAdminClientMock();

    when(bigtableTableAdminClient.exists(anyString())).thenReturn(false);
    when(bigtableTableAdminClient.createTable(any())).thenThrow(RuntimeException.class);

    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.createTable(TABLE_ID, ImmutableList.of("cf1")));
  }

  @Test
  public void testCreateTableShouldWorkWhenBigtableDoesNotThrowAnyError() {
    prepareCreateInstanceAdminClientMock();
    prepareCreateTableAdminClientMock();

    when(bigtableTableAdminClient.createTable(any())).thenThrow(RuntimeException.class);

    assertThrows(
        RuntimeException.class, () -> testManager.createTable(TABLE_ID, ImmutableList.of("cf1")));
  }

  @Test
  public void testWriteShouldThrowErrorWhenInstanceDoesNotExist() {
    prepareCreateInstanceAdminClientMock();

    assertThrows(
        IllegalStateException.class,
        () -> testManager.write(RowMutation.create(TABLE_ID, "sample-key")));
    assertThrows(
        IllegalStateException.class,
        () -> testManager.write(ImmutableList.of(RowMutation.create(TABLE_ID, "sample-key"))));
  }

  @Test
  public void testWriteShouldExitEarlyWhenNoRowMutationsGiven() throws IOException {
    prepareCreateInstanceAdminClientMock();
    testManager.createInstance(cluster);

    Mockito.lenient()
        .when(bigtableResourceManagerClientHandler.getBigtableDataClient())
        .thenThrow(RuntimeException.class);

    testManager.write(ImmutableList.of());
  }

  @Test
  public void testWriteShouldThrowErrorWhenDataClientFailsToInstantiate() throws IOException {
    prepareCreateInstanceAdminClientMock();
    testManager.createInstance(cluster);

    when(bigtableResourceManagerClientHandler.getBigtableDataClient()).thenThrow(IOException.class);

    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.write(RowMutation.create(TABLE_ID, "sample-key")));
    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.write(ImmutableList.of(RowMutation.create(TABLE_ID, "sample-key"))));
  }

  @Test
  public void testWriteShouldThrowErrorWhenDataClientFailsToSendMutations() throws IOException {
    prepareCreateInstanceAdminClientMock();
    prepareCreateDataClientMock();
    testManager.createInstance(cluster);

    doThrow(RuntimeException.class).when(bigtableDataClient).mutateRow(any());

    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.write(RowMutation.create(TABLE_ID, "sample-key")));
    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.write(ImmutableList.of(RowMutation.create(TABLE_ID, "sample-key"))));
  }

  @Test
  public void testWriteShouldWorkWhenBigtableDoesNotThrowAnyError() throws IOException {
    prepareCreateInstanceAdminClientMock();
    prepareCreateDataClientMock();
    testManager.createInstance(cluster);

    testManager.write(RowMutation.create(TABLE_ID, "sample-key"));
    testManager.write(ImmutableList.of(RowMutation.create(TABLE_ID, "sample-key")));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenInstanceDoesNotExist() {
    prepareCreateInstanceAdminClientMock();

    assertThrows(IllegalStateException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenTableDoesNotExist() {
    prepareCreateInstanceAdminClientMock();
    testManager.createInstance(cluster);

    assertThrows(IllegalStateException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenDataClientFailsToInstantiate() throws IOException {
    prepareCreateInstanceAdminClientMock();
    prepareCreateTableAdminClientMock();
    testManager.createInstance(cluster);

    when(bigtableTableAdminClient.exists(anyString())).thenReturn(true);
    when(bigtableResourceManagerClientHandler.getBigtableDataClient()).thenThrow(IOException.class);

    assertThrows(BigtableResourceManagerException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenDataClientFailsToReadRows() throws IOException {
    prepareCreateInstanceAdminClientMock();
    prepareCreateTableAdminClientMock();
    prepareCreateDataClientMock();
    testManager.createInstance(cluster);

    when(bigtableTableAdminClient.exists(anyString())).thenReturn(true);
    when(bigtableDataClient.readRows(any())).thenThrow(NotFoundException.class);

    assertThrows(BigtableResourceManagerException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldThrowNullPointerWhenReadRowsReturnsNull() throws IOException {
    prepareCreateInstanceAdminClientMock();
    prepareCreateTableAdminClientMock();
    prepareCreateDataClientMock();
    testManager.createInstance(cluster);

    when(bigtableTableAdminClient.exists(anyString())).thenReturn(true);
    when(bigtableDataClient.readRows(any())).thenReturn(null);

    assertThrows(NullPointerException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldWorkWhenBigtableDoesNotThrowAnyError() throws IOException {
    prepareCreateInstanceAdminClientMock();
    prepareCreateTableAdminClientMock();
    prepareCreateDataClientMock();
    testManager.createInstance(cluster);

    when(bigtableDataClient.readRows(any())).thenReturn(readRows);
    when(bigtableTableAdminClient.exists(anyString())).thenReturn(true);

    testManager.readTable(TABLE_ID);
  }

  @Test
  public void testCleanupAllCallsDeleteInstance() {
    prepareCreateInstanceAdminClientMock();

    doThrow(RuntimeException.class).when(bigtableInstanceAdminClient).deleteInstance(anyString());

    assertThrows(RuntimeException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllClosesInstanceAdminClient() {
    prepareCreateInstanceAdminClientMock();

    doThrow(RuntimeException.class).when(bigtableInstanceAdminClient).close();

    assertThrows(RuntimeException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllClosesTableAdminClient() {
    prepareCreateInstanceAdminClientMock();
    prepareCreateTableAdminClientMock();

    doThrow(RuntimeException.class).when(bigtableTableAdminClient).close();

    assertThrows(RuntimeException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenInstanceFailsToDelete() {
    prepareCreateInstanceAdminClientMock();

    doThrow(RuntimeException.class).when(bigtableInstanceAdminClient).deleteInstance(anyString());

    assertThrows(BigtableResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenInstanceAdminClientFailsToClose() {
    prepareCreateInstanceAdminClientMock();

    doThrow(RuntimeException.class).when(bigtableInstanceAdminClient).close();

    assertThrows(BigtableResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenTableAdminClientFailsToClose() {
    prepareCreateInstanceAdminClientMock();
    prepareCreateTableAdminClientMock();

    doThrow(RuntimeException.class).when(bigtableTableAdminClient).close();

    assertThrows(BigtableResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldWorkWhenBigtableDoesNotThrowAnyError() {
    prepareCreateInstanceAdminClientMock();
    prepareCreateTableAdminClientMock();

    testManager.cleanupAll();
  }
}
