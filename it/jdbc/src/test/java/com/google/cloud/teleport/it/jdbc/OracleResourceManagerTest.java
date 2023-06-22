/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.it.jdbc;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.testcontainers.containers.OracleContainer;

/** Integration tests for {@link OracleResourceManagerTest}. */
@RunWith(JUnit4.class)
public class OracleResourceManagerTest<T extends OracleResourceManager.DefaultOracleContainer> {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private T container;

  private OracleResourceManager testManager;

  private static final String TEST_ID = "test_id";
  private static final int DEFAULT_ORACLE_INTERNAL_PORT = 1521;
  private static final int ORACLE_EXTERNAL_PORT = 1111;
  private static final String DATABASE = "database";

  @Before
  public void setUp() {
    when(container.withUsername(any())).thenReturn(container);
    when(container.withPassword(any())).thenReturn(container);
    when(container.withDatabaseName(anyString())).thenReturn(container);
    when(container.getDatabaseName()).thenReturn(DATABASE);

    testManager = new OracleResourceManager(container, new OracleResourceManager.Builder(TEST_ID));
  }

  @Test
  public void testGetJDBCPortReturnsCorrectValue() {
    assertThat(testManager.getJDBCPort()).isEqualTo(DEFAULT_ORACLE_INTERNAL_PORT);
  }

  @Test
  public void testGetUriReturnsCorrectConnectionString() {
    when(container.getMappedPort(anyInt())).thenReturn(ORACLE_EXTERNAL_PORT);

    assertThat(testManager.getUri())
        .isEqualTo(
            String.format("jdbc:oracle:thin:@localhost:%d/%s", ORACLE_EXTERNAL_PORT, DATABASE));
  }

  @Test
  public void testGetUriReturnsCorrectConnectionStringWhenXE() {
    when(container.getDatabaseName()).thenReturn(DATABASE);
    when(container.getMappedPort(anyInt())).thenReturn(ORACLE_EXTERNAL_PORT);

    OracleResourceManager oracle =
        new OracleResourceManager(container, OracleResourceManager.builder(TEST_ID).usingXE());

    assertThat(oracle.getUri())
        .isEqualTo(
            String.format("jdbc:oracle:thin:@localhost:%d:xe", ORACLE_EXTERNAL_PORT, DATABASE));
  }
}
