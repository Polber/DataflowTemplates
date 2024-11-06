/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.templates.yaml;

import static org.apache.beam.it.gcp.JDBCBaseIT.POSTGRES_DRIVER;

import com.google.cloud.teleport.metadata.YAMLTemplateIntegrationTest;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.it.gcp.JDBCBaseIT;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(YAMLTemplateIntegrationTest.class)
@RunWith(JUnit4.class)
public class JdbcToBigQueryYamlIT extends JdbcToBigQueryYamlBase {

  @Test
  public void testJdbcToBQYamlWithPGTable() throws IOException {
    // Create Postgres Resource manager
    postgresResourceManager = PostgresResourceManager.builder(testName).build();

    // Arrange Postgres-compatible schema
    JDBCResourceManager.JDBCSchema schema = getPostgresSchema();

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        schema,
        postgresResourceManager,
        false,
        Map.of(
            "table",
            testName,
            "driverJars",
            JDBCBaseIT.postgresDriverGCSPath(this::getGcsPath),
            "driverClassName",
            POSTGRES_DRIVER));
  }

  @Test
  @Category(YAMLTemplateIntegrationTest.class)
  public void testJdbcToBQYamlWithMySqlTable() throws IOException {
    // Create MySQL Resource manager
    mySQLResourceManager = MySQLResourceManager.builder(testName).build();

    // Arrange MySQL-compatible schema
    JDBCResourceManager.JDBCSchema schema = getMySqlSchema();

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        schema, mySQLResourceManager, false, Map.of("table", testName, "jdbcType", "mysql"));
  }
}
