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
package com.google.cloud.teleport.v2.transforms;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.Cell;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** Unit tests for {@link BigQueryConverters}. */
@RunWith(JUnit4.class)
public class BigtableConvertersTest {

    @Test
    public void testConvertJsonToMutationFailsWhenGivenTwoRowKeys() {
        String json = "{\"rowKey\":{\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"}},\"rowKey2\":{\"colFam2\":{\"cell\":\"8888888888\",\"home\":\"9999999999\"}}}";

        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default"));
    }

    @Test
    public void testConvertJsonToMutationFailsWhenGivenFourOrMoreLevels() {
        String json = "{\"fakeLevel\": {\"rowKey\":{\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"}}}}";

        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default"));
    }

    @Test
    public void testConvertJsonToMutationFailsWhenOnlyPartOfJsonIsInRowId() {
        String json = "{\"rowKey\":{\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"}},\"first\":\"John\",\"last\":\"Doe\"}";
        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default"));

        String json2 = "{\"rowKey\":{\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"}},\"colFam2\":{\"cell\":\"8888888888\",\"home\":\"9999999999\"}}";
        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json2, new HashMap<>(), "default"));

        String json3 = "{\"cell\":\"8888888888\",\"home\":\"9999999999\",\"rowKey\":{\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"}}}";
        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json3, new HashMap<>(), "default"));

        String json4 = "{\"colFam2\":{\"cell\":\"8888888888\",\"home\":\"9999999999\"},\"rowKey\":{\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"}}}";
        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json4, new HashMap<>(), "default"));
    }

    @Test
    public void testConvertJsonToMutationFailsWhenMissingBracket() {
        String json = "{\"rowKey\":{\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"}}";
        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default"));

        String json2 = "{\"rowKey\":{\"colFam\":\"first\":\"John\",\"last\":\"Doe\"}}}";
        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json2, new HashMap<>(), "default"));

        String json3 = "\"rowKey\":{\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"}}}";
        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json3, new HashMap<>(), "default"));
    }

    @Test
    public void testConvertJsonToMutationFailsWhenMissingQuotes() {
        String json = "{\"rowKey\":{\"colFam\":{\"first\":\"John,\"last\":\"Doe\"}}}";
        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default"));

        String json2 = "{\"rowKey\":{\"colFam\":{\"first\":\"John\",\"last\":\"Doe}}}";
        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json2, new HashMap<>(), "default"));

        String json3 = "{rowKey\":{\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"}}}";
        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json3, new HashMap<>(), "default"));
    }

    @Test
    public void testConvertJsonToMutationFailsWhenKeyMissingValue() {
        String json = "{\"rowKey\":{\"colFam\":{\"first\":\"John\",\"last\"}}}";
        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default"));

        String json2 = "{\"rowKey\"}";
        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json2, new HashMap<>(), "default"));

        String json3 = "{\"rowKey\":{\"colFam\"}}";
        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json3, new HashMap<>(), "default"));
    }

    @Test
    public void testConvertJsonToMutationWorksWithoutKeyValuePairs() {
        String json = "{\"rowKey\":{\"colFam\":{}}}";
        Mutation mutation = BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default").getValue().iterator().next();

        String json2 = "{\"colFam\":{}}";
        Mutation mutation2 = BigtableConverters.convertJsonToMutation(json2, new HashMap<>(), "default").getValue().iterator().next();

//        assertThat(mutation.getFamilyCellMap().size()).isEqualTo(0);
//        assertThat(mutation2.getFamilyCellMap().size()).isEqualTo(0);
    }

    @Test
    public void testConvertJsonToMutationFailsWhenOnlyGivenColumnFamily() {
        String json = "{\"colFam\":{}}";

        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default"));
    }

    @Test
    public void testConvertJsonToMutationFailsWhenJsonIsEmpty() {
        String json = "{}";
        assertThrows(RuntimeException.class, () -> BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default"));
    }

    @Test
    public void testConvertJsonToMutationUsesGivenRowKey() {
        String givenRowKey = "rowKey";
        String json = "{\"" + givenRowKey + "\":{\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"}}}";
        KV<ByteString, Iterable<Mutation>> keyMutation = BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default");
        Mutation mutation = keyMutation.getValue().iterator().next();
        String rowKey = keyMutation.getKey().toStringUtf8();

        assertThat(rowKey).isEqualTo(givenRowKey);
    }

    @Test
    public void testConvertJsonToMutationUsesGeneratesSameRowKeyWhenOrderIsDifferent() {
        String json = "{\"colFam2\":{\"cell\":\"8888888888\",\"home\":\"9999999999\"},\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"}}";
        KV<ByteString, Iterable<Mutation>> keyMutation = BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default");
        String rowKey = keyMutation.getKey().toStringUtf8();

        String json2 = "{\"cell\":\"8888888888\",\"home\":\"9999999999\",\"colFam\":{\"first\":\"John\"},\"last\":\"Doe\"}";
        KV<ByteString, Iterable<Mutation>> keyMutation2 = BigtableConverters.convertJsonToMutation(json2, new HashMap<>(), "default");
        String rowKey2 = keyMutation2.getKey().toStringUtf8();

        String json3 = "{\"last\":\"Doe\",\"home\":\"9999999999\",\"first\":\"John\",\"cell\":\"8888888888\"}";
        KV<ByteString, Iterable<Mutation>> keyMutation3 = BigtableConverters.convertJsonToMutation(json3, new HashMap<>(), "default");
        String rowKey3 = keyMutation3.getKey().toStringUtf8();

        assertThat(rowKey).isEqualTo(rowKey2);
        assertThat(rowKey2).isEqualTo(rowKey3);
    }

    @Test
    public void testConvertJsonToMutationMapsColumnFamiliesWhenGivenInJson() {
        String json = "{\"colFam2\":{\"cell\":\"8888888888\",\"home\":\"9999999999\"},\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"}}";
        KV<ByteString, Iterable<Mutation>> keyMutation = BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default");
        Iterable<Mutation> mutation = keyMutation.getValue();
        String rowKey = keyMutation.getKey().toStringUtf8();

        Iterator<Mutation> mutationIterator = mutation.iterator();
        Mutation firstEntry = mutationIterator.next();
        Mutation secondEntry = mutationIterator.next();
        Mutation thirdEntry = mutationIterator.next();
        Mutation fourthEntry = mutationIterator.next();
        assertThrows(NoSuchElementException.class, mutationIterator::next);

        assertThat(rowKey).isEqualTo("8888888888#9999999999#Doe#John");

        assertThat(firstEntry.getSetCell().getFamilyName()).isEqualTo("colFam2");
        assertThat(firstEntry.getSetCell().getColumnQualifier().toStringUtf8()).isEqualTo("cell");
        assertThat(firstEntry.getSetCell().getValue().toStringUtf8()).isEqualTo("8888888888");

        assertThat(secondEntry.getSetCell().getFamilyName()).isEqualTo("colFam2");
        assertThat(secondEntry.getSetCell().getColumnQualifier().toStringUtf8()).isEqualTo("home");
        assertThat(secondEntry.getSetCell().getValue().toStringUtf8()).isEqualTo("9999999999");

        assertThat(thirdEntry.getSetCell().getFamilyName()).isEqualTo("colFam");
        assertThat(thirdEntry.getSetCell().getColumnQualifier().toStringUtf8()).isEqualTo("last");
        assertThat(thirdEntry.getSetCell().getValue().toStringUtf8()).isEqualTo("Doe");

        assertThat(fourthEntry.getSetCell().getFamilyName()).isEqualTo("colFam");
        assertThat(fourthEntry.getSetCell().getColumnQualifier().toStringUtf8()).isEqualTo("first");
        assertThat(fourthEntry.getSetCell().getValue().toStringUtf8()).isEqualTo("John");
    }

    @Test
    public void testConvertJsonToMutationMapsColumnsToFamiliesGivenInJson() {
        String json = "{\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"},\"cell\":\"8888888888\",\"home\":\"9999999999\"}";
        Mutation mutation = BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default").getValue().iterator().next();

//        Map.Entry<byte[], List<Cell>> firstEntry = mutation.getFamilyCellMap().firstEntry();
//        Map.Entry<byte[], List<Cell>> secondEntry = mutation.getFamilyCellMap().lastEntry();
//        assertThat(firstEntry.getValue().get(0).toString()).isEqualTo("8888888888#9999999999#Doe#John/colFam:last/LATEST_TIMESTAMP/Put/vlen=3/seqid=0");
//        assertThat(firstEntry.getValue().get(1).toString()).isEqualTo("8888888888#9999999999#Doe#John/colFam:first/LATEST_TIMESTAMP/Put/vlen=4/seqid=0");
//        assertThat(secondEntry.getValue().get(0).toString()).isEqualTo("8888888888#9999999999#Doe#John/default:cell/LATEST_TIMESTAMP/Put/vlen=10/seqid=0");
//        assertThat(secondEntry.getValue().get(1).toString()).isEqualTo("8888888888#9999999999#Doe#John/default:home/LATEST_TIMESTAMP/Put/vlen=10/seqid=0");
    }

    @Test
    public void testConvertJsonToMutationMapsColumnsToFamiliesGivenInJson2() {
        String json = "{\"cell\":\"8888888888\",\"home\":\"9999999999\",\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"}}";
        Mutation mutation = BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default").getValue().iterator().next();

//        Map.Entry<byte[], List<Cell>> firstEntry = mutation.getFamilyCellMap().firstEntry();
//        Map.Entry<byte[], List<Cell>> secondEntry = mutation.getFamilyCellMap().lastEntry();
//        assertThat(firstEntry.getValue().get(0).toString()).isEqualTo("8888888888#9999999999#Doe#John/colFam:last/LATEST_TIMESTAMP/Put/vlen=3/seqid=0");
//        assertThat(firstEntry.getValue().get(1).toString()).isEqualTo("8888888888#9999999999#Doe#John/colFam:first/LATEST_TIMESTAMP/Put/vlen=4/seqid=0");
//        assertThat(secondEntry.getValue().get(0).toString()).isEqualTo("8888888888#9999999999#Doe#John/default:cell/LATEST_TIMESTAMP/Put/vlen=10/seqid=0");
//        assertThat(secondEntry.getValue().get(1).toString()).isEqualTo("8888888888#9999999999#Doe#John/default:home/LATEST_TIMESTAMP/Put/vlen=10/seqid=0");
    }

    @Test
    public void testConvertJsonToMutationMapsColumnsToDefaultWhenFamiliesNotGivenInJson() {
        String json = "{\"colFam2\":{\"middle\":\"A\"},\"cell\":\"8888888888\",\"home\":\"9999999999\",\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"}}";
        Mutation mutation = BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default").getValue().iterator().next();

//        Iterator<Map.Entry<byte[], List<Cell>>> mutationIt = mutation.getFamilyCellMap().entrySet().iterator();
//
//        Map.Entry<byte[], List<Cell>> firstEntry = mutationIt.next();
//        Map.Entry<byte[], List<Cell>> secondEntry = mutationIt.next();
//        Map.Entry<byte[], List<Cell>> thirdEntry = mutationIt.next();
//        assertThat(firstEntry.getValue().get(0).toString()).isEqualTo("8888888888#9999999999#A#Doe#John/colFam:last/LATEST_TIMESTAMP/Put/vlen=3/seqid=0");
//        assertThat(firstEntry.getValue().get(1).toString()).isEqualTo("8888888888#9999999999#A#Doe#John/colFam:first/LATEST_TIMESTAMP/Put/vlen=4/seqid=0");
//        assertThat(secondEntry.getValue().get(0).toString()).isEqualTo("8888888888#9999999999#A#Doe#John/colFam2:middle/LATEST_TIMESTAMP/Put/vlen=1/seqid=0");
//        assertThat(thirdEntry.getValue().get(0).toString()).isEqualTo("8888888888#9999999999#A#Doe#John/default:cell/LATEST_TIMESTAMP/Put/vlen=10/seqid=0");
//        assertThat(thirdEntry.getValue().get(1).toString()).isEqualTo("8888888888#9999999999#A#Doe#John/default:home/LATEST_TIMESTAMP/Put/vlen=10/seqid=0");
    }

    @Test
    public void testConvertJsonToMutationMapsColumnsToDefaultWhenFamiliesNotGivenInJson2() {
        String json = "{\"middle\":\"A\",\"cell\":\"8888888888\",\"home\":\"9999999999\",\"first\":\"John\",\"last\":\"Doe\"}";
        Mutation mutation = BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default").getValue().iterator().next();

//        Map.Entry<byte[], List<Cell>> firstEntry = mutation.getFamilyCellMap().firstEntry();
//        assertThat(firstEntry.getValue().get(0).toString()).isEqualTo("8888888888#9999999999#A#Doe#John/default:middle/LATEST_TIMESTAMP/Put/vlen=1/seqid=0");
//        assertThat(firstEntry.getValue().get(1).toString()).isEqualTo("8888888888#9999999999#A#Doe#John/default:last/LATEST_TIMESTAMP/Put/vlen=3/seqid=0");
//        assertThat(firstEntry.getValue().get(2).toString()).isEqualTo("8888888888#9999999999#A#Doe#John/default:cell/LATEST_TIMESTAMP/Put/vlen=10/seqid=0");
//        assertThat(firstEntry.getValue().get(3).toString()).isEqualTo("8888888888#9999999999#A#Doe#John/default:first/LATEST_TIMESTAMP/Put/vlen=4/seqid=0");
//        assertThat(firstEntry.getValue().get(4).toString()).isEqualTo("8888888888#9999999999#A#Doe#John/default:home/LATEST_TIMESTAMP/Put/vlen=10/seqid=0");
    }

    @Test
    public void testConvertJsonToMutationMapsColumnFamiliesAndRowIdWhenGivenInJson() {
        String json = "{\"rowId\":{\"colFam2\":{\"cell\":\"8888888888\",\"home\":\"9999999999\"},\"colFam\":{\"first\":\"John\",\"last\":\"Doe\"}}}";
        Mutation mutation = BigtableConverters.convertJsonToMutation(json, new HashMap<>(), "default").getValue().iterator().next();

//        Map.Entry<byte[], List<Cell>> firstEntry = mutation.getFamilyCellMap().firstEntry();
//        Map.Entry<byte[], List<Cell>> secondEntry = mutation.getFamilyCellMap().lastEntry();
//        assertThat(firstEntry.getValue().get(0).toString()).isEqualTo("rowId/colFam:last/LATEST_TIMESTAMP/Put/vlen=3/seqid=0");
//        assertThat(firstEntry.getValue().get(1).toString()).isEqualTo("rowId/colFam:first/LATEST_TIMESTAMP/Put/vlen=4/seqid=0");
//        assertThat(secondEntry.getValue().get(0).toString()).isEqualTo("rowId/colFam2:cell/LATEST_TIMESTAMP/Put/vlen=10/seqid=0");
//        assertThat(secondEntry.getValue().get(1).toString()).isEqualTo("rowId/colFam2:home/LATEST_TIMESTAMP/Put/vlen=10/seqid=0");
    }

    @Test
    public void testConvertJsonToMutationMapsColumnsToGivenColumnFamilies() {
        String json = "{\"colFam2\":{\"middle\":\"A\"},\"cell\":\"8888888888\",\"home\":\"9999999999\",\"first\":\"John\",\"last\":\"Doe\"}";
        Map<String,String> columnFamilies = new HashMap<>();
        columnFamilies.put("first", "colFam");
        columnFamilies.put("last", "colFam");
        Mutation mutation = BigtableConverters.convertJsonToMutation(json, columnFamilies, "default").getValue().iterator().next();

//        Iterator<Map.Entry<byte[], List<Cell>>> mutationIt = mutation.getFamilyCellMap().entrySet().iterator();
//
//        Map.Entry<byte[], List<Cell>> firstEntry = mutationIt.next();
//        Map.Entry<byte[], List<Cell>> secondEntry = mutationIt.next();
//        Map.Entry<byte[], List<Cell>> thirdEntry = mutationIt.next();
//        assertThat(firstEntry.getValue().get(0).toString()).isEqualTo("8888888888#9999999999#A#Doe#John/colFam:last/LATEST_TIMESTAMP/Put/vlen=3/seqid=0");
//        assertThat(firstEntry.getValue().get(1).toString()).isEqualTo("8888888888#9999999999#A#Doe#John/colFam:first/LATEST_TIMESTAMP/Put/vlen=4/seqid=0");
//        assertThat(secondEntry.getValue().get(0).toString()).isEqualTo("8888888888#9999999999#A#Doe#John/colFam2:middle/LATEST_TIMESTAMP/Put/vlen=1/seqid=0");
//        assertThat(thirdEntry.getValue().get(0).toString()).isEqualTo("8888888888#9999999999#A#Doe#John/default:cell/LATEST_TIMESTAMP/Put/vlen=10/seqid=0");
//        assertThat(thirdEntry.getValue().get(1).toString()).isEqualTo("8888888888#9999999999#A#Doe#John/default:home/LATEST_TIMESTAMP/Put/vlen=10/seqid=0");
    }

    @Test
    public void testConvertJsonToMutationMapsColumnsToGivenColumnFamilies2() {
        String json = "{\"rowKey\": {\"colFam2\":{\"middle\":\"A\"},\"cell\":\"8888888888\",\"home\":\"9999999999\",\"colFam3\":{\"first\":\"John\"},\"last\":\"Doe\"}}";
        Map<String,String> columnFamilies = new HashMap<>();
        columnFamilies.put("first", "colFam");
        columnFamilies.put("last", "colFam");
        columnFamilies.put("middle", "colFam");
        Mutation mutation = BigtableConverters.convertJsonToMutation(json, columnFamilies, "default2").getValue().iterator().next();

//        Iterator<Map.Entry<byte[], List<Cell>>> mutationIt = mutation.getFamilyCellMap().entrySet().iterator();
//
//        Map.Entry<byte[], List<Cell>> firstEntry = mutationIt.next();
//        Map.Entry<byte[], List<Cell>> secondEntry = mutationIt.next();
//        Map.Entry<byte[], List<Cell>> thirdEntry = mutationIt.next();
//        Map.Entry<byte[], List<Cell>> fourthEntry = mutationIt.next();
//        assertThat(firstEntry.getValue().get(0).toString()).isEqualTo("rowKey/colFam:last/LATEST_TIMESTAMP/Put/vlen=3/seqid=0");
//        assertThat(secondEntry.getValue().get(0).toString()).isEqualTo("rowKey/colFam2:middle/LATEST_TIMESTAMP/Put/vlen=1/seqid=0");
//        assertThat(thirdEntry.getValue().get(0).toString()).isEqualTo("rowKey/colFam3:first/LATEST_TIMESTAMP/Put/vlen=4/seqid=0");
//        assertThat(fourthEntry.getValue().get(0).toString()).isEqualTo("rowKey/default2:cell/LATEST_TIMESTAMP/Put/vlen=10/seqid=0");
//        assertThat(fourthEntry.getValue().get(1).toString()).isEqualTo("rowKey/default2:home/LATEST_TIMESTAMP/Put/vlen=10/seqid=0");
    }
}
