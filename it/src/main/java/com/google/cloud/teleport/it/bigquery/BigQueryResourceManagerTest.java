package com.google.cloud.teleport.it.bigquery;

import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;

/**
 * Test class for DefaultBigQueryResourceManager implementation.
 */
public class BigQueryResourceManagerTest {
    public static void main(String[] args) throws Exception {
        DefaultBigQueryResourceManager rm = new DefaultBigQueryResourceManager("jeff-test-project-templates", "us-central1");
        rm.createDataset("test_dataset_3");
        rm.createTable("test_table", Schema.of(Field.of("test_field", StandardSQLTypeName.STRING)));
        HashMap<String,String> row = new HashMap<String, String>();
        row.put("test_field", "test");
        rm.write("test_table", InsertAllRequest.RowToInsert.of(row));
        rm.createTable("test_table2", Schema.of(Field.of("test_field", StandardSQLTypeName.STRING)));
        row = new HashMap<String, String>();
        row.put("test_field", "test");
        rm.write("test_table2", InsertAllRequest.RowToInsert.of(row));
        ImmutableList<FieldValueList> rows = rm.readTable("test_table");
        for (FieldValueList r : rows) {
            System.out.println(r.toString());
            System.out.println(r.get(0).toString());
        }
        rm.cleanupAll();
    }
}
