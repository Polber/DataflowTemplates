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
package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.it.PipelineUtils.createJobName;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatRecords;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.common.JDBCBaseIT;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.cloud.teleport.it.jdbc.DefaultMSSQLResourceManager;
import com.google.cloud.teleport.it.jdbc.DefaultMySQLResourceManager;
import com.google.cloud.teleport.it.jdbc.DefaultOracleResourceManager;
import com.google.cloud.teleport.it.jdbc.DefaultPostgresResourceManager;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager;
import com.google.cloud.teleport.it.launcher.PipelineLauncher;
import com.google.cloud.teleport.it.launcher.PipelineOperator;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

/** Integration test for {@link JdbcToBigQuery} (JdbcToBigQuery). */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(value = JdbcToBigQuery.class, template = "Jdbc_to_BigQuery")
@RunWith(JUnit4.class)
public class JdbcToBigQueryIT extends JDBCBaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcToBigQueryIT.class);

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String ENTRY_ADDED = "entry_added";

  private static DefaultMySQLResourceManager mySQLResourceManager;
  private static DefaultPostgresResourceManager postgresResourceManager;
  private static DefaultOracleResourceManager oracleResourceManager;
  private static DefaultMSSQLResourceManager msSQLResourceManager;
  private static BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setUp() {
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName, PROJECT)
            .setCredentials(credentials)
            .build();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        mySQLResourceManager,
        msSQLResourceManager,
        postgresResourceManager,
        oracleResourceManager,
        bigQueryResourceManager);
  }

  @Test
  public void testMySqlToBigQuery() throws IOException {
    // Create mySql Resource manager
    mySQLResourceManager = DefaultMySQLResourceManager.builder(testName).build();

    // Arrange mySql-compatible schema
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "BOOLEAN");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        testName, schema, MYSQL_DRIVER, mySqlDriverGCSPath(), mySQLResourceManager);
  }

  @Test
  public void testPostgresToBigQuery() throws IOException {
    // Create postgres Resource manager
    postgresResourceManager = DefaultPostgresResourceManager.builder(testName).build();

    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "INTEGER NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "FLOAT8");
    columns.put(MEMBER, "BOOLEAN");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        testName, schema, POSTGRES_DRIVER, postgresDriverGCSPath(), postgresResourceManager);
  }

  @Test
  public void testOracleToBigQuery() throws IOException {
    // Oracle image does not work on M1
    if (System.getProperty("testOnM1") != null) {
      LOG.info("M1 is being used, Oracle tests are not being executed.");
      return;
    }

    // Create oracle Resource manager
    oracleResourceManager = DefaultOracleResourceManager.builder(testName).build();

    // Arrange oracle-compatible schema
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "BIT NOT NULL");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        testName, schema, ORACLE_DRIVER, oracleDriverGCSPath(), oracleResourceManager);
  }

  @Test
  public void testMsSqlToBigQuery() throws IOException {
    // Create msSql Resource manager
    msSQLResourceManager = DefaultMSSQLResourceManager.builder(testName).build();

    // Arrange msSql-compatible schema
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "BIT NOT NULL");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        testName, schema, MSSQL_DRIVER, msSqlDriverGCSPath(), msSQLResourceManager);
  }

  private void simpleJdbcToBigQueryTest(
      String testName,
      JDBCResourceManager.JDBCSchema schema,
      String driverClassName,
      String driverJars,
      JDBCResourceManager jdbcResourceManager)
      throws IOException {
    // Arrange
    jdbcResourceManager.createTable(testName, schema);
    jdbcResourceManager.write(
        testName,
        getJdbcData(!(jdbcResourceManager instanceof DefaultMSSQLResourceManager)),
        com.google.common.collect.ImmutableList.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED));

    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of(ROW_ID, StandardSQLTypeName.INT64),
            Field.of(NAME, StandardSQLTypeName.STRING),
            Field.of(AGE, StandardSQLTypeName.FLOAT64),
            Field.of(MEMBER, StandardSQLTypeName.BOOL),
            Field.of(ENTRY_ADDED, StandardSQLTypeName.STRING));
    Schema bqSchema = Schema.of(bqSchemaFields);

    bigQueryResourceManager.createDataset(REGION);
    bigQueryResourceManager.createTable(testName, bqSchema);
    String tableSpec = PROJECT + ":" + bigQueryResourceManager.getDatasetId() + "." + testName;

    String jobName = createJobName(testName);
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(jobName, specPath)
            .addParameter("connectionURL", jdbcResourceManager.getUri())
            .addParameter("driverClassName", driverClassName)
            .addParameter("query", "SELECT * FROM " + testName)
            .addParameter("outputTable", tableSpec)
            .addParameter("driverJars", driverJars)
            .addParameter("bigQueryLoadingTemporaryDirectory", getGcsBasePath() + "/temp")
            .addParameter("username", jdbcResourceManager.getUsername())
            .addParameter("password", jdbcResourceManager.getPassword());

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThat(info.state()).isIn(PipelineLauncher.JobState.ACTIVE_STATES);

    PipelineOperator.Result result =
        new PipelineOperator(launcher()).waitUntilDoneAndFinish(createConfig(info));

    // Assert
    assertThat(result).isEqualTo(PipelineOperator.Result.LAUNCH_FINISHED);

    final DateTimeFormatter formatter =
        new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.MILLI_OF_SECOND, 0, 6, true)
            .toFormatter()
            .withZone(ZoneId.systemDefault());

    assertThatRecords(bigQueryResourceManager.readTable(testName))
        .hasRecordsUnordered(jdbcResourceManager.readTable(testName));
  }

  /**
   * Helper function for generating data according to the common schema for the IT's
   *
   * @param useBool Signals whether to use true/false for boolean values instead of bits.
   * @return A map containing the rows of data to be stored in each JDBC table.
   */
  private Map<Integer, List<Object>> getJdbcData(boolean useBool) {
    Map<Integer, List<Object>> data = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      List<Object> values = new ArrayList<>();
      values.add(i);
      values.add(RandomStringUtils.randomAlphabetic(10));
      values.add(1.1 * new Random().nextInt(100));
      values.add(useBool ? i % 2 == 0 : i % 2);
      values.add(Instant.now().toString());
      data.put(i, values);
    }

    return data;
  }
}
