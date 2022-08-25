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
package com.google.cloud.teleport.it.bigquery;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default class for implementation of {@link BigQueryResourceManager} interface.
 *
 * <p>The class supports one dataset, and multiple tables per dataset object.
 *
 * <p>The class is thread-safe.</p>
 */
public final class DefaultBigQueryResourceManager implements BigQueryResourceManager {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultBigQueryResourceManager.class);

    private static final String DEFAULT_CREDENTIAL_ENV_VAR = "DT_IT_ACCESS_TOKEN";

    private final String projectId;
    private final String region;

    private final BigQuery bigQuery;
    private Dataset dataset;

    @VisibleForTesting
    DefaultBigQueryResourceManager(BigQuery bigQuery, String projectId, String region) {
        this.bigQuery = bigQuery;
        this.projectId = projectId;
        this.region = region;
    }

    @VisibleForTesting
    DefaultBigQueryResourceManager(String projectId, String region) {
        this(DefaultBigQueryResourceManager.builder(projectId, region));
    }

    private DefaultBigQueryResourceManager(Builder builder) {
        this(
                BigQueryOptions.newBuilder().setProjectId(builder.projectId).setCredentials(new GoogleCredentials(new AccessToken(System.getenv(DEFAULT_CREDENTIAL_ENV_VAR), null))).build().getService(),
                builder.projectId,
                builder.region
        );
    }

    public static Builder builder(String projectId, String region) {
        return new Builder(projectId, region);
    }

    /**
     * Helper method for determining if a dataset exists in the resource manager.
     *
     * @throws IllegalStateException if a dataset does not exist.
     */
    private void checkForDataset() throws IllegalStateException {
        if (dataset == null) {
            throw new IllegalStateException("Dataset for integration tests has not been created. Please create a dataset first using the 'createDataset()' function");
        }
    }

    /**
     * Helper method for fetching a table stored in the dataset given a table name.
     *
     * @param tableName the name of the table to fetch.
     * @return the table, if it exists.
     * @throws IllegalStateException if the given table name does not exist in the dataset.
     */
    private synchronized Table getTableIfExists(String tableName) throws IllegalStateException {
        checkForDataset();
        Table table = dataset.get(tableName);
        if (table == null) {
            throw new IllegalStateException("Table " + tableName + " has not been created. Please create a table first using the 'createTable()' function");
        }
        return table;
    }

    /**
     * Helper method for logging individual errors thrown by inserting rows to a table. This method is
     * used to log errors thrown by inserting certain rows when other rows were successful.
     *
     * @param insertErrors the map of errors to log.
     */
    private void logInsertErrors(Map<Long,List<BigQueryError>> insertErrors) {
        for (Map.Entry<Long, List<BigQueryError>> entries : insertErrors.entrySet()) {
            long index = entries.getKey();
            for (BigQueryError error : entries.getValue()) {
                LOG.info("Error when inserting row with index {}: {}", index, error.getMessage());
            }
        }
    }

    @Override
    public synchronized void createDataset(String datasetId) {
        if (dataset != null) {
            LOG.info("Already using dataset {}", dataset.getDatasetId().getDataset());
            return;
        }

        try {
            String fullDatasetName = BigQueryResourceManagerUtils.generateDatasetId(datasetId);
            DatasetInfo datasetInfo = DatasetInfo.newBuilder(fullDatasetName).setLocation(this.region).build();
            this.dataset = bigQuery.create(datasetInfo);

        } catch (Exception e) {
            throw new BigQueryResourceManagerException("Failed to create dataset.", e);
        }

        LOG.info("Dataset {} created successfully", datasetId);
    }

    @Override
    public synchronized void createTable(String tableName, Schema schema) {
        checkForDataset();

        LOG.info("Creating table using tableName '{}'.", tableName);

        try {
            TableId tableId = TableId.of(dataset.getDatasetId().getDataset(), tableName);
            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
            bigQuery.create(tableInfo);

        } catch (Exception e) {
            throw new BigQueryResourceManagerException("Failed to create table.", e);
        }

        LOG.info("Successfully created table {}.{}", dataset.getDatasetId().getDataset(), tableName);
    }

    @Override
    public synchronized void write(String tableName, RowToInsert row) {
        write(tableName, ImmutableList.of(row));
    }

    @Override
    public synchronized void write(String tableName, List<RowToInsert> rows) {
        checkForDataset();
        Table table = getTableIfExists(tableName);

        LOG.info("Attempting to write {} records to {}.{}.", rows.size(), dataset.getDatasetId().getDataset(), tableName);

        int successfullyWrittenRecords = rows.size();
        try {
            InsertAllResponse insertResponse = table.insert(rows);
            successfullyWrittenRecords -= insertResponse.getInsertErrors().size();

            if (insertResponse.hasErrors()) {
                LOG.info("Errors encountered when inserting rows: ");
                logInsertErrors(insertResponse.getInsertErrors());
            }

        } catch (Exception e) {
            throw new BigQueryResourceManagerException("Failed to write to table.", e);
        }

        LOG.info("Successfully wrote {} records to {}.{}.", successfullyWrittenRecords, dataset.getDatasetId().getDataset(), tableName);
    }

    @Override
    public synchronized ImmutableList<FieldValueList> readTable(String tableName) {
        checkForDataset();
        Table table = getTableIfExists(tableName);

        // List to store fetched rows
        ImmutableList.Builder<FieldValueList> immutableListBuilder = ImmutableList.builder();

        LOG.info("Reading all rows from {}.{}", dataset.getDatasetId().getDataset(), tableName);

        try {

            TableResult results = bigQuery.listTableData(table.getTableId());
            for (FieldValueList row : results.getValues()) {
                immutableListBuilder.add(row);
            }

        } catch (Exception e) {
            throw new BigQueryResourceManagerException("Failed to read from table.", e);
        }

        ImmutableList<FieldValueList> tableRecords = immutableListBuilder.build();
        LOG.info("Loaded {} records from {}.{}", tableRecords.size(), dataset.getDatasetId().getDataset(), tableName);

        return tableRecords;
    }

    @Override
    public synchronized void cleanup() {
        LOG.info("Attempting to cleanup manager.");

        try {
            if (dataset != null) {
                Page<Table> tables = bigQuery.listTables(dataset.getDatasetId());
                for (Table table : tables.iterateAll()) {
                    bigQuery.delete(TableId.of(projectId, dataset.getDatasetId().getDataset(), table.getTableId().getTable()));
                }
                bigQuery.delete(dataset.getDatasetId());
            }

        } catch (Exception e) {
            throw new BigQueryResourceManagerException("Failed to delete resources.", e);
        }

        LOG.info("Manager successfully cleaned up.");
    }

    /** Builder for {@link DefaultBigQueryResourceManager}. */
    public static final class Builder {

        private final String projectId;
        private final String region;

        private Builder(String projectId, String region) {
            this.projectId = projectId;
            this.region = region;
        }

        public String getProjectId() {
            return projectId;
        }

        public String getRegion() {
            return region;
        }

        public DefaultBigQueryResourceManager build() {
            return new DefaultBigQueryResourceManager(this);
        }
    }
}