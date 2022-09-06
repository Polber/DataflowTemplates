///*
// * Copyright (C) 2022 Google LLC
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not
// * use this file except in compliance with the License. You may obtain a copy of
// * the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing permissions and limitations under
// * the License.
// */
package com.google.cloud.teleport.v2.templates;

import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.cdc.mappers.BigtableMappers;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.BigtableConverters.BigtableTableConfigManager;
import com.google.cloud.teleport.v2.transforms.PubSubToFailSafeElement;
import com.google.cloud.teleport.v2.transforms.UDFTextTransformer;
import com.google.cloud.teleport.v2.transforms.UDFTextTransformer.InputUDFOptions;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Splitter;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PubSubCdcToBigtable {

    /** The log to output status messages to. */
    private static final Logger LOG = LoggerFactory.getLogger(PubSubCdcToBigtable.class);

    /** The default suffix for error tables if dead letter table is not specified. */
    public static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

    /** Pubsub message/string coder for pipeline. */
    public static final FailsafeElementCoder<PubsubMessage, String> CODER =
            FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

    /** String/String Coder for FailsafeElement. */
    public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
            FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    public interface Options extends PipelineOptions, InputUDFOptions {
        @Description(
                "The Cloud Pub/Sub subscription to consume from. "
                        + "The name should be in the format of "
                        + "projects/<project-id>/subscriptions/<subscription-name>.")
        String getInputSubscription();

        void setInputSubscription(String value);

        @Description("GCP Project Id of where to write the Bigtable rows")
        @Validation.Required
        String getBigtableWriteProjectId();

        void setBigtableWriteProjectId(String value);

        @Description("Bigtable Instance id")
        @Validation.Required
        String getBigtableWriteInstanceId();

        void setBigtableWriteInstanceId(String value);

        @Description("Bigtable app profile")
        @Default.String("default")
        String getBigtableWriteAppProfile();

        void setBigtableWriteAppProfile(String value);

        @Description("Bigtable table id")
        @Validation.Required
        String getBigtableWriteTableId();

        void setBigtableWriteTableId(String value);

        @Description("Bigtable column families mapper")
        String getBigtableColumnFamiliesMapper();

        void setBigtableColumnFamiliesMapper(String value);

        @Description("Default Bigtable column family name")
        @Validation.Required
        String getBigtableDefaultWriteColumnFamily();

        void setBigtableDefaultWriteColumnFamily(String value);

        @Description(
                "This determines if new columns and tables should be automatically created in Bigtable")
        @Default.Boolean(true)
        Boolean getAutoMapTables();

        void setAutoMapTables(Boolean value);

        @Description(
                "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
                        + "format. If it doesn't exist, it will be created during pipeline execution.")
        String getOutputDeadletterTable();

        void setOutputDeadletterTable(String value);

        // Dead Letter Queue GCS Directory
        @Description("The Dead Letter Queue GCS Prefix to use for errored data")
        @Default.String("")
        String getDeadLetterQueueDirectory();

        void setDeadLetterQueueDirectory(String value);

        // Window Duration
        @Description("The window duration for DLQ files")
        @Default.String("5s")
        String getWindowDuration();

        void setWindowDuration(String value);

        // Thread Count
        @Description("The number of threads to spawn")
        @Default.Integer(100)
        Integer getThreadCount();

        void setThreadCount(Integer value);
    }

    /**
     * The main entry-point for pipeline execution. This method will start the pipeline but will not
     * wait for its execution to finish. If blocking execution is required, use the {@link
     * PubSubCdcToBigtable#run(Options)} method to start the pipeline and invoke {@code
     * result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        run(options);
    }

    /**
     * Runs the pipeline to completion with the specified options. This method does not wait until the
     * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
     * object to block until the pipeline is finished running if blocking programmatic execution is
     * required.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(Options options) {
        Pipeline pipeline = Pipeline.create(options);
        DeadLetterQueueManager dlqManager = buildDlqManager(options);
        String gcsOutputDateTimeDirectory = null;

        if (options.getDeadLetterQueueDirectory() != null) {
            gcsOutputDateTimeDirectory = dlqManager.getRetryDlqDirectory() + "YYYY/MM/DD/HH/mm/";
        }

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);
        coderRegistry.registerCoderForType(
                FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

        CloudBigtableTableConfiguration bigtableTableConfig =
                new CloudBigtableTableConfiguration.Builder()
                        .withProjectId(options.as(GcpOptions.class).getProject())
                        .withInstanceId(options.getBigtableWriteInstanceId())
                        .withAppProfileId(options.getBigtableWriteAppProfile())
                        .withTableId(options.getBigtableWriteTableId())
                        .build();

        UDFTextTransformer.InputUDFToMutation<String> failsafeMutationTransformer =
                new UDFTextTransformer.InputUDFToMutation<>(
                        options.getBigtableColumnFamiliesMapper() == null
                                ? parseColumnFamilyMapper(options.getBigtableColumnFamiliesMapper())
                                : new HashMap<>(),
                        options.getBigtableDefaultWriteColumnFamily(),
                        options.getJavascriptTextTransformGcsPath(),
                        options.getJavascriptTextTransformFunctionName(),
                        options.getPythonTextTransformGcsPath(),
                        options.getPythonTextTransformFunctionName(),
                        options.getRuntimeRetries(),
                        FAILSAFE_ELEMENT_CODER);

        BigtableTableConfigManager btConfigManager =
                new BigtableTableConfigManager(
                        options.getBigtableWriteProjectId(),
                        options.getBigtableWriteInstanceId(),
                        options.getBigtableWriteTableId());

        /*
         * Steps:
         *  1) Read messages in from Pub/Sub
         *  2) Transform the PubsubMessages into Mutations
         *     - Transform message payload via UDF
         *     - Convert UDF result to Mutation objects
         *  3) Write successful records out to Bigtable
         *     - Automap new objects to Bigtable if enabled
         *     - Write records to Bigtable tables
         *  4) Write failed records out to BigQuery
         */

        /*
         * Step #1: Read messages in from Pub/Sub
         */
        PCollection<PubsubMessage> messages =
                pipeline.apply(
                        "ReadPubSubSubscription",
                        PubsubIO.readMessagesWithAttributes()
                                .fromSubscription(options.getInputSubscription()));

        PCollection<FailsafeElement<String, String>> jsonRecords;

        if (dlqManager != null) {
            PCollection<FailsafeElement<String, String>> failsafeMessages =
                    messages.apply("ConvertPubSubToFailsafe", ParDo.of(new PubSubToFailSafeElement()));
            PCollection<FailsafeElement<String, String>> dlqJsonRecords =
                    pipeline
                            .apply(dlqManager.dlqReconsumer())
                            .apply(
                                    ParDo.of(
                                            new DoFn<String, FailsafeElement<String, String>>() {
                                                @ProcessElement
                                                public void processElement(ProcessContext context) {
                                                    String input = context.element();
                                                    context.output(FailsafeElement.of(input, input));
                                                }
                                            }))
                            .setCoder(FAILSAFE_ELEMENT_CODER);
            jsonRecords = PCollectionList.of(failsafeMessages).and(dlqJsonRecords).apply(Flatten.pCollections());
        }
        else {
            jsonRecords = messages.apply("ConvertPubSubToFailsafe", ParDo.of(new PubSubToFailSafeElement()));
        }

        /*
         * Step #2: Transform the Pub/Sub Messages into Mutations
         */
        PCollectionTuple convertedTableMutations =
                jsonRecords
//                        .apply(
//                                Reshuffle.<FailsafeElement<String, String>>viaRandomKey()
//                                        .withNumBuckets(options.getThreadCount()))
                        .apply("ApplyUdfAndConvertToMutation", failsafeMutationTransformer);

        /*
         * Step #3: Write the successful records out to BigQuery
         *   Either extract table destination only
         *   or extract table destination and auto-map new columns
         */
        PCollection<KV<ByteString, Iterable<Mutation>>> tableEvents;
        if (options.getAutoMapTables()) {
            tableEvents = convertedTableMutations.get(failsafeMutationTransformer.transformOut).apply("Map Data to Bigtable Tables", BigtableMappers.buildBigtableTableMapper(btConfigManager.getProjectId(), btConfigManager.instanceId, btConfigManager.tableId));
        } else {
            tableEvents = convertedTableMutations.get(failsafeMutationTransformer.transformOut);
        }

        /*
         * Step #3: Cont.
         *    - Write rows out to BigQuery
         */
        tableEvents.apply("WriteSuccessfulRecords",
                BigtableIO.write()
                        .withTableId(options.getBigtableWriteTableId())
                        .withProjectId(options.getBigtableWriteProjectId())
                        .withInstanceId(options.getBigtableWriteInstanceId())
                        .withoutValidation());

        return pipeline.run();
    }

    public static Map<String, String> parseColumnFamilyMapper(String columnFamilyMapper) {
        // determine the start and end indices of custom delimiter within the first 2 '^' characters, if given
        int delimitStart = columnFamilyMapper.indexOf("^");
        int delimitEnd = -1;
        if (delimitStart == 0 && delimitStart+1 < columnFamilyMapper.length()-1) {
            delimitEnd = columnFamilyMapper.substring(delimitStart+1).indexOf("^") + 1;
        }

        // entire input is delimiter, so treat as empty parameter -> null
        if (delimitEnd == columnFamilyMapper.length()-1) {
            return null;
        }

        // extract delimiter and remove delimiter definition from string
        String delimiter = delimitStart == 0 && delimitEnd > 0 ? columnFamilyMapper.substring(delimitStart+1, delimitEnd) : "&";
        columnFamilyMapper = delimitStart == 0 && delimitEnd > 0 ? columnFamilyMapper.substring(delimitEnd+1) : columnFamilyMapper;

        try {
            // attempt to map the parameter string to a map with the key being the colFam and the val being the list of applicable columns
            Map<String, String> splitMap = Splitter.on(delimiter).withKeyValueSeparator('=').split(columnFamilyMapper);
            Map<String, List<String>> colFamToColListMap = splitMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> Arrays.asList(e.getValue().split(","))));

            // reverse map so that each col is the key with their respective colFam as the val
            Map<String, String> colToColFamMap = new HashMap<>();
            for (String key : colFamToColListMap.keySet()) {
                for (String val : colFamToColListMap.get(key)) {
                    colToColFamMap.put(val, key);
                }
            }
            return colToColFamMap;

        } catch (Exception e) {
            throw new IllegalArgumentException("Column family mapper parameter is in incorrect format: " + columnFamilyMapper, e);
        }
    }

    private static DeadLetterQueueManager buildDlqManager(Options options) {
        if (options.getDeadLetterQueueDirectory() != null) {
            String tempLocation =
                    options.as(PipelineOptions.class).getTempLocation().endsWith("/")
                            ? options.as(PipelineOptions.class).getTempLocation()
                            : options.as(PipelineOptions.class).getTempLocation() + "/";

            String dlqDirectory =
                    options.getDeadLetterQueueDirectory().isEmpty()
                            ? tempLocation + "dlq/"
                            : options.getDeadLetterQueueDirectory();

            return DeadLetterQueueManager.create(dlqDirectory);
        } else {
            return null;
        }
    }
}
