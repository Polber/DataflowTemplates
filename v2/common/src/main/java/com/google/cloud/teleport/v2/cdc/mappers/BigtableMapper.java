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
package com.google.cloud.teleport.v2.cdc.mappers;

import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.teleport.v2.utils.BigtableTableCache;
import com.google.cloud.teleport.v2.utils.GCSUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BigtableMapper is intended to be easily extensible to enable Bigtable schema management during
 * pipeline execution. New fields and tables will be automatically added to Bigtable when they are
 * detected and before data causes BQ load failures.
 *
 * <p>The BigtableMapper can be easily extended by overriding: - public TableId getTableId(InputT
 * input) - public TableRow getTableRow(InputT input) - public OutputT getOutputObject(InputT input)
 * - public Map<String, StandardSQLTypeName> getInputSchema(TableId tableId, TableRow row)
 */
public class BigtableMapper<InputT, OutputT>
        extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableMapper.class);
    private BigtableTableAdminClient btTableAdminClient;
    private Set<String> ignoreFields = new HashSet<String>();
    private int mapperRetries = 5;
    private final String projectId;
    private final String instanceId;
    private static BigtableTableCache tableCache;
    private static final Cache<String, String> tableLockMap =
            CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();

    public BigtableMapper(String projectId, String instanceId) {
        this.projectId = projectId;
        this.instanceId = instanceId;
    }

    public String getTableId(InputT input) {
        return null;
    }

    public Iterable<Mutation> getRowMutations(InputT input) {
        return null;
    }

    public OutputT getOutputObject(InputT input) {
        return null;
    }

    public void setMapperRetries(int retries) {
        this.mapperRetries = retries;
    }

    public int getMapperRetries() {
        return this.mapperRetries;
    }

    public String getProjectId() {
        return this.projectId;
    }

    public String getInstanceId() {
        return this.instanceId;
    }

    /**
     * The function {@link withIgnoreFields} sets a list of fields to be ignored when mapping new
     * columns to a Bigtable Table.
     *
     * @param fields A Set of fields to ignore.
     */
    public BigtableMapper<InputT, OutputT> withIgnoreFields(Set<String> fields) {
        this.ignoreFields = fields;

        return this;
    }

    private synchronized void setUpTableCache() {
        if (tableCache == null) {
            tableCache =
                    (BigtableTableCache)
                            new BigtableTableCache(btTableAdminClient)
                                    .withCacheResetTimeUnitValue(60)
                                    .withCacheNumRetries(3);
        }
    }

    /** Sets all objects needed during mapper execution. */
    public void setUp() throws IOException {
        if (this.btTableAdminClient == null) {
            this.btTableAdminClient = BigtableTableAdminClient.create(BigtableTableAdminSettings.newBuilder().setProjectId(getProjectId()).setInstanceId(getInstanceId()).build());
        }
        if (tableCache == null) {
            setUpTableCache();
        }
    }

    @Override
    public PCollection<OutputT> expand(PCollection<InputT> tableKVPCollection) {
        return tableKVPCollection.apply(
                "TableRowExtractDestination",
                MapElements.via(
                        new SimpleFunction<InputT, OutputT>() {
                            @Override
                            public OutputT apply(InputT input) {
                                /*
                                    We run validation against every event to ensure all columns
                                    exist in source.
                                    If a column is in the event and not in Bigtable,
                                    the column is added to the table before the event can continue.
                                */
                                try {
                                    setUp();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                String tableId = getTableId(input);
                                Iterable<Mutation> rowMutations = getRowMutations(input);
                                int retries = getMapperRetries();

                                applyMapperToTableRow(tableId, rowMutations, retries);
                                return getOutputObject(input);
                            }
                        }));
    }

    /**
     * Extracts and applies new column information to Bigtable by comparing the TableRow against the
     * Bigtable Table. Retries the supplied number of times before failing.
     *
     * @param tableId a TableId referencing the Bigtable table to be loaded to.
     * @param rowMutations a TableRow with the raw data to be loaded into Bigtable.
     * @param retries Number of remaining retries before error is raised.
     */
    private void applyMapperToTableRow(String tableId, Iterable<Mutation> rowMutations, int retries) {
        try {
            updateTableIfRequired(tableId, rowMutations);
        } catch (Exception e) {
            if (retries > 0) {
                try {
                    int sleepSecs = (getMapperRetries() - retries + 1) * 5;
                    LOG.info(
                            "Mapper Retry {} Remaining: {}: {}",
                            retries,
                            tableId,
                            e.toString());
                    Thread.sleep(sleepSecs);
                    applyMapperToTableRow(tableId, rowMutations, retries - 1);
                } catch (InterruptedException i) {
                    LOG.info("Mapper Retries Interrupted: {}: {}", tableId, e.toString());
                    throw e;
                }
            } else {
                LOG.info("Mapper Retries Exceeded: {}: {}", tableId, e.toString());
                throw e;
            }
        }
    }

    /**
     * Extracts and applies new column information to Bigtable by comparing the TableRow against the
     * Bigtable Table.
     *
     * @param tableId a TableId referencing the Bigtable table to be loaded to.
     * @param rowMutations a TableRow with the raw data to be loaded into Bigtable.
     */
    private void updateTableIfRequired(String tableId, Iterable<Mutation> rowMutations) {
        Table table = tableCache.getOrCreateBigtableTable(tableId);

        Set<String> newColumnFamiliesList = getNewColumnFamilies(rowMutations, table, this.ignoreFields);

        if (newColumnFamiliesList.size() > 0) {
            LOG.info("Updating Table: {}", tableId);
            updateBigtableTable(tableId, rowMutations, this.ignoreFields);
        }
    }

    private static String getTableLock(String tableId) {
        String tableLock = tableLockMap.getIfPresent(tableId);
        if (tableLock != null) {
            return tableLock;
        }

        synchronized (tableLockMap) {
            tableLock = tableLockMap.getIfPresent(tableId);
            if (tableLock != null) {
                return tableLock;
            }

            tableLockMap.put(tableId, tableId);
            return tableId;
        }
    }

    /* Update Bigtable Table Object Supplied */
    private void updateBigtableTable(String tableId, Iterable<Mutation> rowMutations, Set<String> ignoreFields) {
        synchronized (getTableLock(tableId)) {
            Table table = tableCache.get(tableId);

            // Add all new column families to the list
            Set<String> columnFamiliesToAdd = getNewColumnFamilies(rowMutations, table, this.ignoreFields);

            // Create request to modify table
            if (columnFamiliesToAdd.size() > 0) {
                LOG.info("Mapping New Columns for: {} -> {}", tableId, columnFamiliesToAdd);
                ModifyColumnFamiliesRequest request = ModifyColumnFamiliesRequest.of(tableId);
                for (String colFam : columnFamiliesToAdd) {
                    request.addFamily(colFam);
                }
                btTableAdminClient.modifyFamilies(request);
                LOG.info("Updated Table: {}", tableId);

                tableCache.reset(tableId, table);
            }
        }
    }

    public static Set<String> getNewColumnFamilies(
            Iterable<Mutation> rowMutations,
            Table table,
            Set<String> ignoreFields) {

        HashSet<String> columnFamiliesToAdd = new HashSet<>();
        for (Mutation mutation : rowMutations) {
            String columnFamily = mutation.getSetCell().getFamilyName();
            columnFamiliesToAdd.add(columnFamily);
        }

        // Remove already existing column families
        HashSet<String> existingColFamilies = table.getColumnFamilies().stream().map(ColumnFamily::getId).collect(Collectors.toCollection(HashSet::new));
        columnFamiliesToAdd.removeAll(existingColFamilies);

        return columnFamiliesToAdd;
    }
}
