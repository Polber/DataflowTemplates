package com.google.cloud.teleport.v2.utils;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigtableTableCache extends MappedObjectCache<String, Table> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableCache.class);
    private final BigtableTableAdminClient btTableAdminClient;

    public BigtableTableCache(BigtableTableAdminClient btTableAdminClient) {
        this.btTableAdminClient = btTableAdminClient;
    }

    /**
     * Returns {@code Table} after creating the table with no columns in BigQuery if required.
     *
     * @param tableId a TableId referencing the BigQuery table being requested.
     */
    public Table getOrCreateBigtableTable(String tableId) {
        Table table = this.cachedObjects.getIfPresent(tableId);
        if (table != null) {
            return table;
        }

        synchronized (this) {
            table = this.reset(tableId, null);
            if (table != null) {
                return table;
            }
            // Create Blank BigQuery Table
            CreateTableRequest createTableRequest = CreateTableRequest.of(tableId);
            table = btTableAdminClient.createTable(createTableRequest);
        }

        return table;
    }

    @Override
    public Table getObjectValue(String key) {
        LOG.info("BigQueryTableCache: Get mapped object cache {}", key);
        try {
            return this.btTableAdminClient.getTable(key);
        } catch (NotFoundException e) {
            return null;
        }
    }
}
