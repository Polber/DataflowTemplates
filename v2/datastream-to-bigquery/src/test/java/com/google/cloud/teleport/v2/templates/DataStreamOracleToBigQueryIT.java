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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.teleport.it.gcp.kms.KMSResourceManager;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

public class DataStreamOracleToBigQueryIT {

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String ENTRY_ADDED = "entry_added";

  private static final String ORACLE_USERNAME =
      "CiQAhXAMVUuJJ4M101nyCA3KSFIIkW/5gZ8YtSjSg8NuVdntrF8SLwB3eJxfBEdow0GuD0BCZdI4bnpdymBJTSnSfA8sTjI6WzV2cR2NyZd5C32CIPNh";
  private static final String ORACLE_PASSWORD =
      "CiQAhXAMVc0T5GD/m/QZ9irA2ZG8Mua2VC7kXitvyy25j+z9gIkSLwB3eJxfXYi+KQWLz3veC1S9Y1RpO9+7eU/yLPgkE4gvgOI2QrXXcRHNwQNFJ5lO";
  private static final String ORACLE_DATABASE_SERVICE =
      "CiQAhXAMVXef6u+h3Ky9wK1kA9bXztliEyuuD5FS4b+GJuSrs7ASKwB3eJxf7UXR28ct3M7RncUKNeIgaqdJztVzp9G8Cn/LVlHVbguzZ9PykSk=";
  private static final String ORACLE_HOST =
      "CiQAhXAMVSVeOrgdTpiLL1UzhvO2qZKFqH6jVguIIXgSq9R51qkSNAB3eJxfcjaAxT+snnnw2LrBLpfhV3FT5svQZdnGOwLdOCCwOl7+Eyj7zAeBfaOgEL8uCj8=";
  private static final String ORACLE_PORT =
      "CiQAhXAMVYKDKNn1bHUTF7I/8pK2q43IjpLUQiLtVUYgwJU2FbcSLQB3eJxfHsRvsFPKH+TYsZ+aj5V3GLcNchwP1Y5pJt097U22DdJy55vG3jFn+g==";

  public static void main(String[] args) {
    //    System.setProperty("oracle.jdbc.timezoneAsRegion", "false");
    //
    //    AbstractJDBCResourceManager<?> oracle =
    //        OracleResourceManager.builder("test")
    //            .usingSid()
    //            .setUsername("system")
    //            .setPassword("oracle")
    //            .setDatabaseName("HR")
    //            .setHost("10.128.0.90")
    //            .setPort(1521)
    //            .useStaticContainer()
    //            .build();
    //
    //    HashMap<String, String> columns = new HashMap<>();
    //    columns.put(ROW_ID, "NUMERIC NOT NULL");
    //    columns.put(NAME, "VARCHAR(200)");
    //    columns.put(AGE, "NUMERIC");
    //    columns.put(MEMBER, "VARCHAR(200)");
    //    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    //    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns,
    // ROW_ID);
    //
    //    String tableName = "DataStreamOracleToBigQueryIT";
    //    oracle.createTable(tableName, schema);
    //    oracle.write(tableName, getJdbcData(List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED)));
    //
    //    System.out.println(oracle.readTable(tableName));
    //
    //    oracle.cleanupAll();
    KMSResourceManager kms = KMSResourceManager.builder("cloud-teleport-testing").build();
    Function<String, String> encrypt =
        message -> kms.encrypt("nokill-oracle-db", "connection-details", message);
    Function<String, String> decrypt =
        message -> kms.decrypt("nokill-oracle-db", "connection-details", message);
    CryptoKey cryptoKey = kms.getOrCreateCryptoKey("nokill-oracle-db", "connection-details");

//    System.out.println(decrypt.apply(ORACLE_USERNAME));
//    System.out.println(decrypt.apply(ORACLE_PASSWORD));
//    System.out.println(decrypt.apply(ORACLE_DATABASE_SERVICE));
//    System.out.println(decrypt.apply(ORACLE_HOST));
//    System.out.println(decrypt.apply(ORACLE_PORT));
//    System.out.println(encrypt.apply("datastream-private-connect-us-central1"));
//    System.out.println(decrypt.apply(ORACLE_PORT));
    System.out.println(encrypt.apply("SYSTEM"));
  }

  private static List<Map<String, Object>> getJdbcData(List<String> columns) {
    List<Map<String, Object>> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Map<String, Object> values = new HashMap<>();
      values.put(columns.get(0), i);
      values.put(columns.get(1), RandomStringUtils.randomAlphabetic(10));
      values.put(columns.get(2), new Random().nextInt(100));
      values.put(columns.get(3), i % 2 == 0 ? "Y" : "N");
      values.put(columns.get(4), Instant.now().toString());
      data.add(values);
    }

    return data;
  }
}
