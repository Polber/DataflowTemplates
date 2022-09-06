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

import com.google.api.services.bigquery.model.TableRow;
import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BigQueryMappers contains different versions of a generic BigQueryMapper class. The BigQueryMapper
 * can be easily extended by overriding: - public TableId getTableId(InputT input) - public TableRow
 * getTableRow(InputT input) - public OutputT getOutputObject(InputT input) - public Map<String,
 * LegacySQLTypeName> getInputSchema(InputT input)
 *
 * <p>BigQueryMapper Versions can be used via helper functions buildBigQueryTableMapper(String
 * datasetProvider, String tableNameProvider) - This expects table name to be provided and handles
 * schema changes for a TableRow object
 *
 * <p>buildBigQueryDynamicTableMapper - Expect to process a KV<TableId, TableRow> and the class will
 * manage the schema - for each table supplied in the stream
 */
public class BigtableMappers {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableMappers.class);

    public BigtableMappers() {}

    /*** Section 1: Functions to build Mapper Class for each different required input ***/
    /* Build Static TableRow BigQuery Mapper */
    public static BigtableMapper<KV<ByteString, Iterable<Mutation>>, KV<ByteString, Iterable<Mutation>>> buildBigtableTableMapper(
            String projectId, String instanceId, String tableId) {
        return new BigtableTableMapper(instanceId, tableId, projectId);
    }

    /** Section 2: Extended Mapper Classes implemented for different input types. */
    public static class BigtableTableMapper extends BigtableMapper<KV<ByteString, Iterable<Mutation>>, KV<ByteString, Iterable<Mutation>>> {

        private final String tableId;

        public BigtableTableMapper(
                String instanceId, String tableId, String projectId) {
            super(projectId, instanceId);

            this.tableId = tableId;
        }

        @Override
        public String getTableId(KV<ByteString, Iterable<Mutation>> input) {
            return tableId;
        }

        @Override
        public Iterable<Mutation> getRowMutations(KV<ByteString, Iterable<Mutation>> input) {
            return input.getValue();
        }

        @Override
        public KV<ByteString, Iterable<Mutation>> getOutputObject(KV<ByteString, Iterable<Mutation>> input) {
            return input;
        }
    }
}
