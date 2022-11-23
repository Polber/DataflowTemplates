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
package com.google.cloud.teleport.v2.mongodb.templates;

import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.bigtable.DefaultBigtableResourceManager;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient;
import com.google.cloud.teleport.it.dataflow.FlexTemplateClient;
import com.google.cloud.teleport.it.mongodb.DefaultMongoDBResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

/** Integration test for {@link MongoDbToBigQuery} (MongoDB_to_BigQuery). */
@TemplateIntegrationTest(MongoDbToBigQuery.class)
@RunWith(JUnit4.class)
public final class MongoDbToBigQueryIT extends TemplateTestBase {

  @Rule public final TestName testName = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(DefaultBigtableResourceManager.class);
  private static final String HOST_IP = TestProperties.hostIp();

  private static final String MONGO_URI = "mongoDbUri";
  private static final String MONGO_DB = "database";
  private static final String MONGO_COLLECTION = "collection";
  private static final String BIGQUERY_TABLE = "outputTableSpec";
  private static final String USER_OPTION = "userOption";

  private static final String MONGO_DB_ID = "_id";

  private static DefaultMongoDBResourceManager mongoDbClient;
  private static BigQueryResourceManager bigQueryClient;

  @Before
  public void setup() throws IOException {
    mongoDbClient =
        DefaultMongoDBResourceManager.builder(testName.getMethodName()).setHost(HOST_IP).build();

    bigQueryClient =
        DefaultBigQueryResourceManager.builder(testName.getMethodName(), PROJECT)
            .setCredentials(credentials)
            .build();
  }

  @After
  public void tearDownClass() {
    boolean producedError = false;

    try {
      mongoDbClient.cleanupAll();
    } catch (Exception e) {
      LOG.error("Failed to delete MongoDB resources. {}", e.getMessage());
      producedError = true;
    }

    try {
      bigQueryClient.cleanupAll();
    } catch (Exception e) {
      LOG.error("Failed to delete MongoDB resources. {}", e.getMessage());
      producedError = true;
    }

    if (producedError) {
      throw new IllegalStateException("Failed to delete resources. Check above for errors.");
    }
  }

  @Test
  public void testMongoDbToBigQuery() throws IOException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);

    String collectionName = testName.getMethodName();
    List<Document> mongoDocuments = generateDocuments();
    mongoDbClient.insertDocuments(collectionName, mongoDocuments);

    String bqTable = testName.getMethodName();
    List<Field> bqSchemaFields = new ArrayList<>();
    mongoDocuments
        .get(0)
        .forEach((key, val) -> bqSchemaFields.add(Field.of(key, StandardSQLTypeName.STRING)));
    Schema bqSchema = Schema.of(bqSchemaFields);

    bigQueryClient.createDataset(REGION);
    bigQueryClient.createTable(bqTable, bqSchema);
    String tableSpec = PROJECT + ":" + bigQueryClient.getDatasetId() + "." + bqTable;

    // Act
    DataflowTemplateClient.LaunchConfig options =
        DataflowTemplateClient.LaunchConfig.builder(jobName, specPath)
            .addParameter(MONGO_URI, mongoDbClient.getUri())
            .addParameter(MONGO_DB, mongoDbClient.getDatabaseName())
            .addParameter(MONGO_COLLECTION, collectionName)
            .addParameter(BIGQUERY_TABLE, tableSpec)
            .addParameter(USER_OPTION, "FLATTEN")
            .build();
    DataflowTemplateClient dataflow =
        FlexTemplateClient.builder().setCredentials(credentials).build();

    DataflowTemplateClient.JobInfo info = dataflow.launchTemplate(PROJECT, REGION, options);
    assertThat(info.state()).isIn(DataflowTemplateClient.JobState.ACTIVE_STATES);

    DataflowOperator.Result result =
        new DataflowOperator(dataflow)
            .waitForConditionAndFinish(
                createConfig(info), () -> bigQueryClient.readTable(bqTable).getTotalRows() != 0);

    // Assert
    assertThat(result).isEqualTo(DataflowOperator.Result.CONDITION_MET);

    Map<String, JSONObject> mongoMap = new HashMap<>();
    mongoDocuments.forEach(
        mongoDocument -> {
          JSONObject mongoDbJson = new JSONObject(mongoDocument.toJson());
          mongoMap.put(mongoDbJson.getJSONObject(MONGO_DB_ID).getString("$oid"), mongoDbJson);
        });

    TableResult tableRows = bigQueryClient.readTable(bqTable);
    tableRows
        .getValues()
        .forEach(
            row ->
                row.forEach(
                    val -> {
                      JSONObject bigQueryJson = new JSONObject(val.getStringValue());
                      JSONObject bigQueryIdJson =
                          new JSONObject(
                              bigQueryJson.getString(MONGO_DB_ID).replaceFirst("=", ":"));
                      String bigQueryOid = bigQueryIdJson.getString("$oid");
                      bigQueryJson.put(MONGO_DB_ID, bigQueryIdJson);

                      assertThat(mongoMap.get(bigQueryOid).toString())
                          .isEqualTo(bigQueryJson.toString());
                    }));
  }

  private static DataflowOperator.Config createConfig(DataflowTemplateClient.JobInfo info) {
    return DataflowOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .build();
  }

  private static List<Document> generateDocuments() {
    int numDocuments = Integer.parseInt(getProperty("numDocs", "100"));
    int numFields = Integer.parseInt(getProperty("numFields", "20"));
    int maxEntryLength = Integer.parseInt(getProperty("maxEntryLength", "20"));
    List<Document> mongoDocuments = new ArrayList<>();

    List<String> mongoDocumentKeys = new ArrayList<>();
    for (int j = 0; j < numFields; j++) {
      mongoDocumentKeys.add(
          RandomStringUtils.randomAlphabetic(1)
              + RandomStringUtils.randomAlphanumeric(0, maxEntryLength - 1));
    }

    for (int i = 0; i < numDocuments; i++) {
      Document randomDocument = new Document().append(MONGO_DB_ID, new ObjectId());

      for (int j = 0; j < numFields; j++) {
        randomDocument.append(
            mongoDocumentKeys.get(j), RandomStringUtils.randomAlphanumeric(0, 20));
      }

      mongoDocuments.add(randomDocument);
    }

    return mongoDocuments;
  }

  private static String getProperty(String name, String defaultValue) {
    String value = System.getProperty(name);
    return value != null ? value : defaultValue;
  }
}
