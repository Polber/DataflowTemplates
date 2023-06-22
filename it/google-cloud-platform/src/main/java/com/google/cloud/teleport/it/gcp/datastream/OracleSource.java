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
package com.google.cloud.teleport.it.gcp.datastream;

import com.google.cloud.datastream.v1.OracleRdbms;
import com.google.cloud.datastream.v1.OracleSchema;
import com.google.cloud.datastream.v1.OracleSourceConfig;
import com.google.cloud.datastream.v1.OracleTable;
import com.google.protobuf.MessageOrBuilder;

/**
 * Client for Oracle resource used by Datastream.
 *
 * <p>Subclass of {@link JDBCSource}.
 */
public class OracleSource extends JDBCSource {

  private static final String DEFAULT_ORACLE_DATABASE_SERVICE = "XE";

  private final String databaseService;

  OracleSource(Builder builder) {
    super(builder);
    this.databaseService = builder.databaseService;
  }

  public static Builder builder(String hostname, String username, String password, int port) {
    return new Builder(hostname, username, password, port);
  }

  @Override
  public SourceType type() {
    return SourceType.ORACLE;
  }

  @Override
  public MessageOrBuilder config() {
    OracleSourceConfig.Builder configBuilder = OracleSourceConfig.newBuilder();
    if (this.allowedTables().size() > 0) {
      OracleRdbms.Builder oracleRdmsBuilder = OracleRdbms.newBuilder();
      for (String schema : this.allowedTables().keySet()) {
        OracleSchema.Builder oracleSchemaBuilder = OracleSchema.newBuilder().setSchema(schema);
        for (String table : this.allowedTables().get(schema)) {
          oracleSchemaBuilder.addOracleTables(OracleTable.newBuilder().setTable(table));
        }
        oracleRdmsBuilder.addOracleSchemas(oracleSchemaBuilder);
      }
      configBuilder.setIncludeObjects(oracleRdmsBuilder);
    }
    return configBuilder.build();
  }

  public String databaseService() {
    return databaseService;
  }

  /** Builder for {@link OracleSource}. */
  public static class Builder extends JDBCSource.Builder<OracleSource> {

    private String databaseService;

    private Builder(String hostname, String username, String password, int port) {
      super(hostname, username, password, port);
      this.databaseService = DEFAULT_ORACLE_DATABASE_SERVICE;
    }

    public Builder setDatabaseService(String databaseService) {
      this.databaseService = databaseService;
      return this;
    }

    @Override
    public OracleSource build() {
      return new OracleSource(this);
    }
  }
}
