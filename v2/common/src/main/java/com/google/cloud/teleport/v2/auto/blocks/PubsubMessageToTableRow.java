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
package com.google.cloud.teleport.v2.auto.blocks;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.auto.Consumes;
import com.google.cloud.teleport.metadata.auto.DlqOutputs;
import com.google.cloud.teleport.metadata.auto.Outputs;
import com.google.cloud.teleport.v2.auto.blocks.PubsubMessageToTableRow.TransformOptions;
import com.google.cloud.teleport.v2.auto.dlq.BigQueryDeadletterOptions;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.NonNull;

@AutoService(SchemaTransformProvider.class)
public class PubsubMessageToTableRow
    extends TemplateTransformClass<
        TransformOptions, PubsubMessageToTableRow.PubsubMessageToTableRowTransformConfiguration> {

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class PubsubMessageToTableRowTransformConfiguration extends Configuration {

    abstract String getJavascriptTextTransformGcsPath();

    abstract String getJavascriptTextTransformFunctionName();

    public void validate() {
      String invalidConfigMessage = "Invalid PubSubMessageToTableRow configuration: ";
      if (this.getErrorHandling() != null) {
        checkArgument(
            !Strings.isNullOrEmpty(this.getErrorHandling().getOutput()),
            invalidConfigMessage + "Output must not be empty if error handling specified.");
      }
    }

    public static PubsubMessageToTableRowTransformConfiguration.Builder builder() {
      return new AutoValue_PubsubMessageToTableRow_PubsubMessageToTableRowTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder extends Configuration.Builder<Builder> {

      public abstract PubsubMessageToTableRowTransformConfiguration.Builder
          setJavascriptTextTransformGcsPath(String path);

      public abstract PubsubMessageToTableRowTransformConfiguration.Builder
          setJavascriptTextTransformFunctionName(String name);

      public abstract PubsubMessageToTableRowTransformConfiguration build();
    }

    public static PubsubMessageToTableRowTransformConfiguration fromOptions(
        TransformOptions options) {
      return new AutoValue_PubsubMessageToTableRow_PubsubMessageToTableRowTransformConfiguration
              .Builder()
          .setJavascriptTextTransformGcsPath(options.getJavascriptTextTransformGcsPath())
          .setJavascriptTextTransformFunctionName(options.getJavascriptTextTransformFunctionName())
          .build();
    }
  }

  @Override
  public @NonNull Class<PubsubMessageToTableRowTransformConfiguration> configurationClass() {
    return PubsubMessageToTableRowTransformConfiguration.class;
  }

  @Override
  public @NonNull String identifier() {
    return "blocks:external:org.apache.beam:pubsub_to_bigquery:v1";
  }

  public interface TransformOptions
      extends JavascriptTextTransformerOptions, BigQueryDeadletterOptions {

    @TemplateParameter.BigQueryTable(
        order = 1,
        description = "BigQuery output table",
        helpText =
            "BigQuery table location to write the output to. The table's schema must match the "
                + "input JSON objects.")
    String getOutputTableSpec();

    void setOutputTableSpec(String tableSpec);
  }

  /** The tag for the main output for the UDF. */
  public static final TupleTag<FailsafeElement<PubsubMessage, String>> UDF_OUT =
      new TupleTag<>() {};

  /** The tag for the main output of the json transformation. */
  public static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<>() {};

  /** The tag for the dead-letter output of the udf. */
  public static final TupleTag<FailsafeElement<PubsubMessage, String>> UDF_DEADLETTER_OUT =
      new TupleTag<>() {};

  /** The tag for the dead-letter output of the json to table row transform. */
  public static final TupleTag<FailsafeElement<PubsubMessage, String>> TRANSFORM_DEADLETTER_OUT =
      new TupleTag<>() {};

  private static final FailsafeElementCoder<PubsubMessage, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(PubsubMessageWithAttributesAndMessageIdCoder.of()),
          NullableCoder.of(StringUtf8Coder.of()));

  @Consumes(
      value = Row.class,
      types = {RowTypes.PubSubMessageRow.class})
  @Outputs(
      value = Row.class,
      types = {RowTypes.SchemaTableRow.class})
  @DlqOutputs(
      value = Row.class,
      types = {RowTypes.FailsafePubSubRow.class})
  public PCollectionRowTuple transform(PCollectionRowTuple input, TransformOptions options) {
    return transform(input, PubsubMessageToTableRowTransformConfiguration.fromOptions(options));
  }

  public PCollectionRowTuple transform(
      PCollectionRowTuple input, PubsubMessageToTableRowTransformConfiguration config) {
    PCollectionTuple udfOut =
        input
            .get(BlockConstants.OUTPUT_TAG)
            // Map the incoming messages into FailsafeElements so we can recover from failures
            // across multiple transforms.
            .apply("MapToRecord", ParDo.of(new PubSubMessageToFailsafeElementFn()))
            .setCoder(FAILSAFE_ELEMENT_CODER)
            .apply(
                "InvokeUDF",
                JavascriptTextTransformer.FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                    .setFileSystemPath(config.getJavascriptTextTransformGcsPath())
                    .setFunctionName(config.getJavascriptTextTransformFunctionName())
                    .setSuccessTag(UDF_OUT)
                    .setFailureTag(UDF_DEADLETTER_OUT)
                    .build());

    PCollectionTuple jsonToTableRowOut =
        udfOut
            .get(UDF_OUT)
            .setCoder(FAILSAFE_ELEMENT_CODER)
            .apply(
                "JsonToTableRow",
                FailsafeJsonToTableRow.<PubsubMessage>newBuilder()
                    .setSuccessTag(TRANSFORM_OUT)
                    .setFailureTag(TRANSFORM_DEADLETTER_OUT)
                    .build());

    PCollectionList<FailsafeElement<PubsubMessage, String>> pcs =
        PCollectionList.of(udfOut.get(UDF_DEADLETTER_OUT).setCoder(FAILSAFE_ELEMENT_CODER))
            .and(jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT).setCoder(FAILSAFE_ELEMENT_CODER));

    PCollection<Row> outputRows =
        jsonToTableRowOut
            .get(TRANSFORM_OUT)
            .apply(
                MapElements.into(TypeDescriptor.of(Row.class))
                    .via(RowTypes.SchemaTableRow::TableRowToRow))
            .setCoder(RowCoder.of(RowTypes.SchemaTableRow.SCHEMA));

    PCollection<Row> errors =
        pcs.apply(Flatten.pCollections())
            .apply(
                MapElements.into(TypeDescriptor.of(Row.class))
                    .via(RowTypes.FailsafePubSubRow::FailsafePubSubRowToRow))
            .setCoder(RowCoder.of(RowTypes.FailsafePubSubRow.SCHEMA));

    return PCollectionRowTuple.of(BlockConstants.OUTPUT_TAG, outputRows)
        .and(BlockConstants.ERROR_TAG, errors);
  }

  private static class PubSubMessageToFailsafeElementFn
      extends DoFn<Row, FailsafeElement<PubsubMessage, String>> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message =
          RowTypes.PubSubMessageRow.RowToPubSubMessage(Objects.requireNonNull(context.element()));
      context.output(
          FailsafeElement.of(message, new String(message.getPayload(), StandardCharsets.UTF_8)));
    }
  }

  @Override
  public Class<TransformOptions> getOptionsClass() {
    return TransformOptions.class;
  }
}
