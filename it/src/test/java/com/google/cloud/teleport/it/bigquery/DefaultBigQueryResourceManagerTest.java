package com.google.cloud.teleport.it.bigquery;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/** Unit tests for {@link com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager}. */
@RunWith(JUnit4.class)
public class DefaultBigQueryResourceManagerTest {

    @Rule
    public final MockitoRule mockito = MockitoJUnit.rule();

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private BigQuery bigQuery;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Dataset dataset;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Table table;
    @Mock
    private Page<Table> tables;
    @Mock
    private Schema schema;
    @Mock
    private InsertAllRequest.RowToInsert row;

    private static final String TABLE_NAME = "table-name";
    private static final String DATASET_ID = "dataset-id";
    private static final String PROJECT_ID = "test-project";
    private static final String REGION = "us-central1";

    private DefaultBigQueryResourceManager testManager;

    @Before
    public void setUp() {
        testManager = new DefaultBigQueryResourceManager(bigQuery, PROJECT_ID, REGION);
    }

    private void prepareCreateDataset() {
        when(bigQuery.create((DatasetInfo) any())).thenReturn(dataset);
        Mockito.lenient().when(dataset.getDatasetId().getDataset()).thenReturn(DATASET_ID);
    }

    private void prepareCreateTable() {
        when(bigQuery.create((TableInfo) any())).thenReturn(table);
        Mockito.lenient().when(table.getTableId().getTable()).thenReturn(TABLE_NAME);
    }

    @Test
    public void testCreateDatasetShouldThrowErrorWhenDatasetCreateFails() {
        when(bigQuery.create((DatasetInfo) any())).thenThrow(RuntimeException.class);

        assertThrows(BigQueryResourceManagerException.class, () -> testManager.createDataset(DATASET_ID));
    }

    @Test
    public void testCreateDatasetShouldNotCreateDatasetWhenDatasetAlreadyExists() {
        prepareCreateDataset();
        testManager.createDataset(DATASET_ID);
        Mockito.lenient().when(bigQuery.create((DatasetInfo) any())).thenThrow(RuntimeException.class);

        testManager.createDataset(DATASET_ID);
    }

    @Test
    public void testCreateDatasetShouldWorkWhenBigQueryDoesNotThrowAnyError() {
        prepareCreateDataset();
        testManager.createDataset(DATASET_ID);
    }

    @Test
    public void testCreateTableShouldThrowErrorWhenDatasetDoesNotExist() {
        assertThrows(IllegalStateException.class, () -> testManager.createTable(TABLE_NAME, schema));
    }

    @Test
    public void testCreateTableShouldThrowErrorWhenCreateFails() {
        prepareCreateDataset();
        testManager.createDataset(DATASET_ID);

        when(bigQuery.create((TableInfo) any())).thenThrow(BigQueryException.class);

        assertThrows(BigQueryResourceManagerException.class, () -> testManager.createTable(TABLE_NAME, schema));
    }

    @Test
    public void testCreateTableShouldWorkWhenBigQueryDoesNotThrowAnyError() {
        prepareCreateDataset();
        prepareCreateTable();
        testManager.createDataset(DATASET_ID);

        testManager.createTable(TABLE_NAME, schema);
    }

    @Test
    public void testWriteShouldThrowErrorWhenDatasetDoesNotExist() {
        when(bigQuery.create((DatasetInfo) any())).thenThrow(RuntimeException.class);

        assertThrows(IllegalStateException.class, () -> testManager.write(TABLE_NAME, row));
        assertThrows(IllegalStateException.class, () -> testManager.write(TABLE_NAME, ImmutableList.of(row)));
    }

    @Test
    public void testWriteShouldThrowErrorWhenTableDoesNotExist() {
        prepareCreateDataset();
        testManager.createDataset(DATASET_ID);

        when(dataset.get(anyString())).thenReturn(null);

        assertThrows(IllegalStateException.class, () -> testManager.write(TABLE_NAME, row));
        assertThrows(IllegalStateException.class, () -> testManager.write(TABLE_NAME, ImmutableList.of(row)));
    }

    @Test
    public void testWriteShouldThrowErrorWhenInsertFails() {
        prepareCreateDataset();
        testManager.createDataset(DATASET_ID);

        when(dataset.get(anyString())).thenReturn(table);
        when(table.insert(any())).thenThrow(BigQueryException.class);

        assertThrows(BigQueryResourceManagerException.class, () -> testManager.write(TABLE_NAME, row));
        assertThrows(BigQueryResourceManagerException.class, () -> testManager.write(TABLE_NAME, ImmutableList.of(row)));
    }

    @Test
    public void testWriteShouldWorkWhenBigQueryDoesNotThrowAnyError() {
        prepareCreateDataset();
        testManager.createDataset(DATASET_ID);

        when(dataset.get(anyString())).thenReturn(table);

        testManager.write(TABLE_NAME, row);
        testManager.write(TABLE_NAME, ImmutableList.of(row));
    }

    @Test
    public void testCleanupShouldThrowErrorWhenTableDeleteFails() {
        prepareCreateDataset();
        prepareCreateTable();
        testManager.createDataset(DATASET_ID);

        when(tables.iterateAll()).thenReturn(ImmutableList.of(table));
        when(bigQuery.listTables((DatasetId) any())).thenReturn(tables);
        when(bigQuery.delete((TableId) any())).thenThrow(BigQueryException.class);

        assertThrows(BigQueryResourceManagerException.class, () -> testManager.cleanupAll());
    }

    @Test
    public void testCleanupShouldThrowErrorWhenDatasetDeleteFails() {
        prepareCreateDataset();
        prepareCreateTable();
        testManager.createDataset(DATASET_ID);

        when(tables.iterateAll()).thenReturn(ImmutableList.of(table));
        when(bigQuery.listTables((DatasetId) any())).thenReturn(tables);
        when(bigQuery.delete((DatasetId) any())).thenThrow(BigQueryException.class);

        assertThrows(BigQueryResourceManagerException.class, () -> testManager.cleanupAll());
    }

    @Test
    public void testCleanupShouldWorkWhenBigQueryDoesNotThrowAnyError() {
        prepareCreateDataset();
        prepareCreateTable();
        testManager.createDataset(DATASET_ID);

        when(tables.iterateAll()).thenReturn(ImmutableList.of(table));
        when(bigQuery.listTables((DatasetId) any())).thenReturn(tables);

        testManager.cleanupAll();
    }

}
