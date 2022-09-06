package com.google.cloud.teleport.v2.transforms;

import com.google.auto.value.AutoValue;
import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.lang.Math.min;

public class BigtableConverters {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableConverters.class);

    private static final int MAX_ROW_KEY_LENGTH = 4095;

    public static KV<ByteString, Iterable<Mutation>> convertJsonToMutation(String json, Map<String, String> columnFamilies, String defaultColumnFamily) {
        try {
            // Convert json string to JSONObject for parsing
            JSONObject jsonObj = new JSONObject(json);
            if (json.equals("{}")) {
                throw new RuntimeException("json message is empty: " + json);
            }
            String rowKeyToUse = generateRowKey(jsonObj);
            JSONObject jsonObjToUse = jsonObj;

            // Check depth of JSON
            boolean hasRowKey = false;
            for (Iterator<String> jsonObjKeys = jsonObj.keys(); jsonObjKeys.hasNext(); ) {
                String maybeRowKey = jsonObjKeys.next();
                JSONObject rowKeyLevel = jsonObj.optJSONObject(maybeRowKey);
                if (rowKeyLevel != null) {
                    // json has at least 2 levels
                    for (Iterator<String> rowKeyLevelKeys = rowKeyLevel.keys(); rowKeyLevelKeys.hasNext(); ) {
                        JSONObject colFamLevel = rowKeyLevel.optJSONObject(rowKeyLevelKeys.next());
                        if (colFamLevel != null) {
                            // json has multiple row keys (reached 3 levels multiple times)
                            if (hasRowKey) {
                                throw new RuntimeException("json has more than one row key specified: " + json);
                            }
                            // json has at least 3 levels
                            for (Iterator<String> colFamLevelKeys = colFamLevel.keys(); colFamLevelKeys.hasNext(); ) {
                                if (colFamLevel.optJSONObject(colFamLevelKeys.next()) != null) {
                                    // json has 4 or more levels, so throw error
                                    throw new RuntimeException("json has more than three levels: " + json);
                                }
                            }
                            // Since json has 3 levels, rowKey was given
                            rowKeyToUse = maybeRowKey;
                            jsonObjToUse = rowKeyLevel;
                        }
                    }
                }
                // set hasRowKey to true so all subsequent passes throw error if there are 3+ levels since the rowKey
                // level needs to be the first entry in the JSON
                hasRowKey = true;
            }
            // Since json only has one level, only the key-value pairs are given
            return convertColumnFamilyLevelJsonToMutation(jsonObjToUse, rowKeyToUse, columnFamilies, defaultColumnFamily);

        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize json to mutation: " + e.getMessage() + json, e);
        }
    }

    private static String generateRowKey(JSONObject jsonObj) {
        String rowKey = generateRowKeyRec(jsonObj);
        rowKey = rowKey.substring(min(1, rowKey.length()), min(MAX_ROW_KEY_LENGTH, rowKey.length()));
        String[] rowKeyArr = rowKey.split("#");
        Arrays.sort(rowKeyArr);
        return String.join("#", rowKeyArr);
    }

    private static String generateRowKeyRec(JSONObject jsonObj) {
        if (jsonObj == null) {
            return "#";
        }

        StringBuilder rowKey = new StringBuilder();
        for (Iterator<String> jsonObjKeys = jsonObj.keys(); jsonObjKeys.hasNext(); ) {
            String key = jsonObjKeys.next();
            String rowKeyPrefix = generateRowKeyRec(jsonObj.optJSONObject(key));
            rowKey.append(rowKeyPrefix);
            if (jsonObj.optJSONObject(key) == null) {
                rowKey.append(jsonObj.get(key));
            }
        }

        return rowKey.toString();
    }

    private static KV<ByteString, Iterable<Mutation>> convertColumnFamilyLevelJsonToMutation(JSONObject jsonObj, String rowKey, Map<String, String> columnFamilies, String defaultColumnFamily) {
        List<Mutation> mutations = new ArrayList<>();

        for (Iterator<String> jsonObjKeys = jsonObj.keys(); jsonObjKeys.hasNext(); ) {
            String colKey = jsonObjKeys.next();
            JSONObject innerObj = jsonObj.optJSONObject(colKey);
            if (innerObj != null) {
                for (Iterator<String> innerObjKeys = innerObj.keys(); innerObjKeys.hasNext(); ) {
                    String innerColKey = innerObjKeys.next();
                    Object colVal = innerObj.get(innerColKey);
                    if (colVal != null) {
                        ByteString colValBytes = ByteString.copyFrom(colVal.toString().getBytes());
                        ByteString innerColKeyBytes = ByteString.copyFrom(innerColKey.getBytes());
                        Mutation.SetCell setCell = Mutation.SetCell.newBuilder().setFamilyName(colKey).setColumnQualifier(innerColKeyBytes).setValue(colValBytes).setTimestampMicros(System.currentTimeMillis() * 1000).build();
                        Mutation mutation = Mutation.newBuilder().setSetCell(setCell).build();
                        mutations.add(mutation);
                    }
                }
            } else {
                Object colVal = jsonObj.get(colKey);
                String colFamily = columnFamilies.get(colKey) != null ? columnFamilies.get(colKey) : defaultColumnFamily;
                if (colVal != null) {
                    ByteString colValBytes = ByteString.copyFrom(colVal.toString().getBytes());
                    ByteString colKeyBytes = ByteString.copyFrom(colKey.getBytes());
                    Mutation.SetCell setCell = Mutation.SetCell.newBuilder().setFamilyName(colFamily).setColumnQualifier(colKeyBytes).setValue(colValBytes).setTimestampMicros(System.currentTimeMillis() * 1000).build();
                    Mutation mutation = Mutation.newBuilder().setSetCell(setCell).build();
                    mutations.add(mutation);
                }
            }
        }

        return KV.of(ByteString.copyFrom(rowKey.getBytes()), mutations);
    }

    @AutoValue
    public abstract static class FailsafeJsonToMutation<T>
            extends PTransform<PCollection<FailsafeElement<T, String>>, PCollectionTuple> {

        public static <T> BigtableConverters.FailsafeJsonToMutation.Builder<T> newBuilder() {
            return new AutoValue_BigtableConverters_FailsafeJsonToMutation.Builder<>();
        }

        public abstract TupleTag<KV<ByteString, Iterable<Mutation>>> successTag();

        public abstract TupleTag<FailsafeElement<T, String>> failureTag();

        public abstract Map<String, String> columnFamilies();

        public abstract String defaultColumnFamily();

        @Override
        public PCollectionTuple expand(PCollection<FailsafeElement<T, String>> failsafeElements) {
            return failsafeElements.apply(
                    "JsonToMutation",
                    ParDo.of(
                                    new DoFn<FailsafeElement<T, String>, KV<ByteString, Iterable<Mutation>>>() {
                                        @ProcessElement
                                        public void processElement(ProcessContext context) {
                                            FailsafeElement<T, String> element = context.element();
                                            String json = element.getPayload();
                                            try {
                                                KV<ByteString, Iterable<Mutation>> row = convertJsonToMutation(json, columnFamilies(), defaultColumnFamily());
//                                                LOG.info("json to mutation: " + row.getValue().iterator().next());
                                                context.output(row);
                                            } catch (Exception e) {
                                                context.output(
                                                        failureTag(),
                                                        FailsafeElement.of(element)
                                                                .setErrorMessage(e.getMessage())
                                                                .setStacktrace(Throwables.getStackTraceAsString(e)));
                                            }
                                        }
                                    })
                            .withOutputTags(successTag(), TupleTagList.of(failureTag())));
        }

        /** Builder for {@link BigtableConverters.FailsafeJsonToMutation}. */
        @AutoValue.Builder
        public abstract static class Builder<T> {

            public abstract BigtableConverters.FailsafeJsonToMutation.Builder<T> setSuccessTag(TupleTag<KV<ByteString, Iterable<Mutation>>> successTag);

            public abstract BigtableConverters.FailsafeJsonToMutation.Builder<T> setFailureTag(TupleTag<FailsafeElement<T, String>> failureTag);

            public abstract BigtableConverters.FailsafeJsonToMutation.Builder<T> setColumnFamilies(Map<String, String> columnFamilies);

            public abstract BigtableConverters.FailsafeJsonToMutation.Builder<T> setDefaultColumnFamily(String columnFamily);

            public abstract BigtableConverters.FailsafeJsonToMutation<T> build();
        }
    }

    public static class BigtableTableConfigManager {

        public String projectId;
        public String instanceId;
        public String tableId;

        /**
         * Build a {@code BigtableTableConfigManager} for use in pipelines.
         *
         * @param projectIdVal The BQ Dataset value or a templated value.
         * @param instanceIdVal The BQ Table value or a templated value.
         * @param tableIdVal The full path of a BQ Table ie. `project:dataset.table`
         *     <p>Optionally supply projectIdVal, datasetTemplateVal, and tableTemplateVal or the config
         *     manager will default to using outputTableSpec.
         */
        public BigtableTableConfigManager(
                String projectIdVal,
                String instanceIdVal,
                String tableIdVal) {
            this.projectId = projectIdVal;
            this.instanceId = instanceIdVal;
            this.instanceId = tableIdVal;
        }

        public String getProjectId() {
            return this.projectId;
        }

        public String getInstanceId() {
            return this.instanceId;
        }

        public String getTableId() {
            return this.tableId;
        }
    }
}
