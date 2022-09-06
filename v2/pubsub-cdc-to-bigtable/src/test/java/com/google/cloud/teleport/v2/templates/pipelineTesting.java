package com.google.cloud.teleport.v2.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.cdc.mappers.BigtableMappers;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.transforms.BigQueryDynamicConverters;
import com.google.cloud.teleport.v2.transforms.PubSubToFailSafeElement;
import com.google.cloud.teleport.v2.transforms.UDFTextTransformer;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Splitter;
import com.google.protobuf.ByteString;
import org.apache.beam.runners.core.construction.renderer.PipelineDotRenderer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RunWith(JUnit4.class)
public class pipelineTesting {

    // Our static input data, which will make up the initial PCollection.
    static final PubsubMessage[] WORDS_ARRAY = new PubsubMessage[]{
            new PubsubMessage("Hello".getBytes(), null),
            new PubsubMessage("Hello".getBytes(), null),
            new PubsubMessage("Bye".getBytes(), null),
            new PubsubMessage("Hello".getBytes(), null),
            new PubsubMessage("Bye".getBytes(), null)
    };

    static final List<PubsubMessage> WORDS = Arrays.asList(WORDS_ARRAY);

    private static final Logger LOG = LoggerFactory.getLogger(pipelineTesting.class);

    public static final FailsafeElementCoder<PubsubMessage, String> CODER =
            FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

    /** String/String Coder for FailsafeElement. */
    public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
            FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    // Create a test pipeline.
    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testCountWords() {

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);
        coderRegistry.registerCoderForType(
                FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

        UDFTextTransformer.InputUDFToTableRow<String> failsafeTableRowTransformer =
                new UDFTextTransformer.InputUDFToTableRow<String>(
                        null,
                        null,
                        null,
                        null,
                        5,
                        FAILSAFE_ELEMENT_CODER);

        BigQueryConverters.BigQueryTableConfigManager bqConfigManager =
                new BigQueryConverters.BigQueryTableConfigManager(
                        (String) "jeff-test-project-templates",
                        (String) "jeff_test_dataset",
                        (String) "jeff_test_table",
                        (String) null);

        // Create an input PCollection.
        PCollection<PubsubMessage> messages =
                pipeline.apply(
                        "ReadPubSubSubscription",
                        PubsubIO.readMessagesWithAttributes()
                                .fromSubscription("projects/jeff-test-project-templates/subscriptions/jeff-test-sub"));

        PCollection<FailsafeElement<String, String>> jsonRecords;
        jsonRecords = messages.apply("ConvertPubSubToFailsafe", ParDo.of(new PubSubToFailSafeElement()));

        PCollectionTuple convertedTableRows =
                jsonRecords
                        /*
                         * Step #2: Transform the PubsubMessages into TableRows
                         */
                        .apply(
                                Reshuffle.<FailsafeElement<String, String>>viaRandomKey()
                                        .withNumBuckets(1))
                        .apply("ApplyUdfAndConvertToTableRow", failsafeTableRowTransformer);

        PCollection<KV<TableId, TableRow>> tableEvents;
        tableEvents =
                convertedTableRows
                        .get(failsafeTableRowTransformer.transformOut)
                        .apply(
                                "ExtractBigQueryTableDestination",
                                BigQueryDynamicConverters.extractTableRowDestination(
                                        bqConfigManager.getProjectId(),
                                        bqConfigManager.getDatasetTemplate(),
                                        bqConfigManager.getTableTemplate()));

        WriteResult writeResult =
                tableEvents.apply(
                        "WriteSuccessfulRecords",
                        BigQueryIO.<KV<TableId, TableRow>>write()
                                .to(new BigQueryDynamicConverters().bigQueryDynamicDestination())
                                .withFormatFunction(element -> element.getValue())
                                .withoutValidation()
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withExtendedErrorInfo()
                                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                                .withFailedInsertRetryPolicy(InsertRetryPolicy.alwaysRetry()));

        // Run the pipeline.
        pipeline.run();
    }

    @Test
    public void testBqToBt() {
        PipelineOptions options = pipeline.getOptions();
        options.setTempLocation("gs://bq-to-bt-test/temp/");
//        Pipeline bqPipeline = TestPipeline.create(options);

        CloudBigtableTableConfiguration bigtableTableConfig =
                new CloudBigtableTableConfiguration.Builder()
                        .withProjectId("jeff-test-project-templates")
                        .withInstanceId("jeff-test-instance")
                        .withAppProfileId("default")
                        .withTableId("jeff-test-table")
                        .build();

        pipeline
                .apply(
                        "AvroToMutation",
                        BigQueryIO.read(
                                        BigQueryConverters.AvroToMutation.newBuilder()
                                                .setColumnFamily("names")
                                                .setRowkey("row")
                                                .build())
                                .fromQuery("SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS row, first, last FROM jeff-test-project-templates.jeff_test_dataset.jeff_test_table LIMIT 1000")
                                .withoutValidation()
                                .withTemplateCompatibility()
                                .usingStandardSql());
//                .apply("WriteToTable", CloudBigtableIO.writeToTable(bigtableTableConfig));

        pipeline.run(options);
    }

    private static DeadLetterQueueManager buildDlqManager(PubSubCdcToBigtable.Options options) {
//        if (options.getDeadLetterQueueDirectory() != null) {
//            String tempLocation =
//                    options.as(PipelineOptions.class).getTempLocation().endsWith("/")
//                            ? options.as(PipelineOptions.class).getTempLocation()
//                            : options.as(PipelineOptions.class).getTempLocation() + "/";
//
//            String dlqDirectory =
//                    options.getDeadLetterQueueDirectory().isEmpty()
//                            ? tempLocation + "dlq/"
//                            : options.getDeadLetterQueueDirectory();
//
//            return DeadLetterQueueManager.create(dlqDirectory);
//        } else {
//            return null;
//        }
        return DeadLetterQueueManager.create("gs://jeff-test-bucket-templates/temp/" + "dlq/");
    }

    @Test
    public void testPubsubToBigtable() {

        DeadLetterQueueManager dlqManager = buildDlqManager(null);
        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(
                FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

        CloudBigtableTableConfiguration bigtableTableConfig =
                new CloudBigtableTableConfiguration.Builder()
                        .withProjectId("jeff-test-project-templates")
                        .withInstanceId("jeff-test-instance")
                        .withAppProfileId("default")
                        .withTableId("pubsub-to-bigtable")
                        .build();

        UDFTextTransformer.InputUDFToMutation<String> failsafeMutationTransformer =
                new UDFTextTransformer.InputUDFToMutation<String>(
                        PubSubCdcToBigtable.parseColumnFamilyMapper("^&^names=first,last&fake=fake1,fake2"),
                        "default",
                        null,
                        null,
                        null,
                        null,
                        1,
                        FAILSAFE_ELEMENT_CODER);

        // Create an input PCollection.
        PCollection<PubsubMessage> messages =
                pipeline.apply(
                        "ReadPubSubSubscription",
                        PubsubIO.readMessagesWithAttributes()
                                .fromSubscription("projects/jeff-test-project-templates/subscriptions/jeff-test-sub"));

        PCollection<FailsafeElement<String, String>> jsonRecords;
//        if (true) {
//            PCollection<FailsafeElement<String, String>> failsafeMessages =
//                    messages.apply("ConvertPubSubToFailsafe", ParDo.of(new PubSubToFailSafeElement()));
//            PCollection<FailsafeElement<String, String>> dlqJsonRecords =
//                    pipeline
//                            .apply(dlqManager.dlqReconsumer())
//                            .apply(
//                                    ParDo.of(
//                                            new DoFn<String, FailsafeElement<String, String>>() {
//                                                @ProcessElement
//                                                public void processElement(ProcessContext context) {
//                                                    String input = context.element();
//                                                    context.output(FailsafeElement.of(input, input));
//                                                }
//                                            }))
//                            .setCoder(FAILSAFE_ELEMENT_CODER);
//            jsonRecords = PCollectionList.of(failsafeMessages).and(dlqJsonRecords).apply(Flatten.pCollections());
//        }
//        else {
//            jsonRecords = messages.apply("ConvertPubSubToFailsafe", ParDo.of(new PubSubToFailSafeElement()));
//        }
        jsonRecords = messages.apply("ConvertPubSubToFailsafe", ParDo.of(new PubSubToFailSafeElement()));

        PCollectionTuple convertedTableMutations =
                jsonRecords
                        /*
                         * Step #2: Transform the PubsubMessages into TableRows
                         */
                        .apply(
                                Reshuffle.<FailsafeElement<String, String>>viaRandomKey()
                                        .withNumBuckets(1))
                        .apply("ApplyUdfAndConvertToTableRow", failsafeMutationTransformer);

        PCollection<KV<ByteString, Iterable<Mutation>>> tableEvents;
        if (true) {
            tableEvents = convertedTableMutations
                    .get(failsafeMutationTransformer.transformOut)
                    .apply("Map Data to Bigtable Tables",
                            BigtableMappers.buildBigtableTableMapper(
                                    "jeff-test-project-templates",
                                    "jeff-test-instance",
                                    "pubsub-to-bigtable"));

        } else {
            tableEvents = convertedTableMutations
                    .get(failsafeMutationTransformer.transformOut);
        }

        tableEvents
                .apply("WriteSuccessfulRecords",
                BigtableIO.write()
                    .withTableId("pubsub-to-bigtable")
                    .withProjectId("jeff-test-project-templates")
                    .withInstanceId("jeff-test-instance")
                    .withoutValidation());

//        String dotString = PipelineDotRenderer.toDotString(pipeline);
//        System.out.println(dotString);

        pipeline.run();
    }

    @Test
    public void testPubSubToBq() {
        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);
        coderRegistry.registerCoderForType(
                FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

        UDFTextTransformer.InputUDFToTableRow<String> failsafeTableRowTransformer =
                new UDFTextTransformer.InputUDFToTableRow<String>(
                        null,
                        null,
                        null,
                        null,
                        1,
                        FAILSAFE_ELEMENT_CODER);

        PCollection<PubsubMessage> messages =
                pipeline.apply(
                        "ReadPubSubSubscription",
                        PubsubIO.readMessagesWithAttributes().fromSubscription("projects/jeff-test-project-templates/subscriptions/jeff-test-sub"));

        PCollection<FailsafeElement<String, String>> jsonRecords;
        jsonRecords = messages.apply("ConvertPubSubToFailsafe", ParDo.of(new PubSubToFailSafeElement()));

        PCollectionTuple convertedTableRows =
                jsonRecords
                        /*
                         * Step #2: Transform the PubsubMessages into TableRows
                         */
                        .apply(
                                Reshuffle.<FailsafeElement<String, String>>viaRandomKey()
                                        .withNumBuckets(1))
                        .apply("ApplyUdfAndConvertToTableRow", failsafeTableRowTransformer);

        pipeline.run();
    }


    @Test
    public void testParseMutation() {
        String json = "{\"rowKey\": {\"names\": {\"first\": \"John\", \"last\": \"Doe\"}}}";
        JSONObject jsonObj = new JSONObject(json);

        if (jsonObj.keys().hasNext()) {
            String rowKey = jsonObj.keys().next();
            if (jsonObj.keys().hasNext()) {
                // throw exception
            }
            Put row = new Put(Bytes.toBytes(rowKey));

            jsonObj = jsonObj.getJSONObject(rowKey);
            for (Iterator<String> it = jsonObj.keys(); it.hasNext();) {
                String colFamily = it.next();

                JSONObject colObj = jsonObj.getJSONObject(colFamily);
                for (Iterator<String> iter = colObj.keys(); iter.hasNext(); ) {
                    String colKey = iter.next();
                    Object colVal = colObj.get(colKey);
                    byte[] colValBytes = colVal == null ? null : Bytes.toBytes(colVal.toString());
                    row.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colKey), colValBytes);
                }
            }

            LOG.info(row.toString());
        }
    }

    @Test
    public void testParseTableRow() {
        String json = "{{\"first\": \"John\", \"last\": \"Doe\"}, {\"first\": \"Jane\", \"last\": \"Doe\"}]}";

        TableRow row;
        // Parse the JSON into a {@link TableRow} object.
        try (InputStream inputStream =
                     new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + json, e);
        }

        System.out.println(row);
    }

    @Test
    public void testParseColFam() {
        String columnFamilyMapper = "^%^colFam1=col1,col2%colFam2=col2,col3,col4%colFam3=col5";

        // determine the start and end indices of custom delimiter within the first 2 '^' characters, if given
        int delimitStart = columnFamilyMapper.indexOf("^");
        int delimitEnd = -1;
        if (delimitStart == 0 && delimitStart+1 < columnFamilyMapper.length()-1) {
            delimitEnd = columnFamilyMapper.substring(delimitStart+1).indexOf("^") + 1;
        }

        // entire input is delimiter, so treat as empty parameter -> null
        if (delimitEnd == columnFamilyMapper.length()-1) {
            throw new RuntimeException("Empty string!");
        }

        String delimiter = delimitStart == 0 && delimitEnd > 0 ? columnFamilyMapper.substring(delimitStart+1, delimitEnd) : "&";
        columnFamilyMapper = delimitStart == 0 && delimitEnd > 0 ? columnFamilyMapper.substring(delimitEnd+1) : columnFamilyMapper;

        Map<String, String> map = Splitter.on(delimiter).withKeyValueSeparator('=').split(columnFamilyMapper);
        Map<String, List<String>> newMap = map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> Arrays.asList(e.getValue().split(","))));

        Map<String, String> returnMap = new HashMap<>();
        for (String key : newMap.keySet()) {
            for (String val : newMap.get(key)) {
                returnMap.put(val, key);
            }
        }

        System.out.println(returnMap);
    }
}
