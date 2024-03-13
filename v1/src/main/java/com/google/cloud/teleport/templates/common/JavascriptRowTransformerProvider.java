package com.google.cloud.teleport.templates.common;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.base.Throwables;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.extensions.protobuf.ProtoByteUtils;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.script.ScriptException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.cloud.teleport.templates.common.JavascriptTextTransformer.getJavascriptRuntime;

@AutoService(SchemaTransformProvider.class)
public class JavascriptRowTransformerProvider
    extends TypedSchemaTransformProvider<JavascriptRowTransformerProvider.Configuration> {

  protected static final String INPUT_ROWS_TAG = "input";
  protected static final String OUTPUT_ROWS_TAG = "output";

  static final String PAYLOAD_TAG = "payload";

  static final String VALID_FORMATS_STR = "RAW,AVRO,JSON,PROTO";
  static final Set<String> VALID_DATA_FORMATS =
      Sets.newHashSet(VALID_FORMATS_STR.split(","));

  @Override
  protected Class<Configuration> configurationClass() {
    return Configuration.class;
  }

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return null;
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:yaml:javascript_row_transform:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_ROWS_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_ROWS_TAG);
  }


  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {

    public abstract String getJavascriptTextTransformGcsPath();

    public abstract String getJavascriptTextTransformFunctionName();

    @Nullable
    public abstract Integer getJavascriptTextTransformReloadIntervalMinutes();

    @Nullable
    public abstract Boolean getLoggingEnabled();

    @SchemaFieldDescription(
        "The encoding format for the data stored in Kafka. Valid options are: " + VALID_FORMATS_STR)
    @Nullable
    public abstract String getFormat();

    @SchemaFieldDescription(
        "The schema in which the data is encoded in the Kafka topic. "
            + "For AVRO data, this is a schema defined with AVRO schema syntax "
            + "(https://avro.apache.org/docs/1.10.2/spec.html#schemas). "
            + "For JSON data, this is a schema defined with JSON-schema syntax (https://json-schema.org/). "
            + "If a URL to Confluent Schema Registry is provided, then this field is ignored, and the schema "
            + "is fetched from Confluent Schema Registry.")
    @Nullable
    public abstract String getSchema();

    @SchemaFieldDescription(
        "The path to the Protocol Buffer File Descriptor Set file. This file is used for schema"
            + " definition and message serialization.")
    @Nullable
    public abstract String getFileDescriptorPath();

    @SchemaFieldDescription(
        "The name of the Protocol Buffer message to be used for schema"
            + " extraction and data conversion.")
    @Nullable
    public abstract String getMessageName();

    @Nullable
    public abstract ErrorHandling getErrorHandling();

    public void validate() {
      final String dataFormat = this.getFormat();
      assert dataFormat == null || VALID_DATA_FORMATS.contains(dataFormat)
          : "Valid data formats are " + VALID_DATA_FORMATS;

      final String inputSchema = this.getSchema();
      final String messageName = this.getMessageName();
      final String fileDescriptorPath = this.getFileDescriptorPath();

      if (dataFormat != null && dataFormat.equals("RAW")) {
        assert inputSchema == null : "To read from Kafka in RAW format, you can't provide a schema.";
      } else if (dataFormat != null && dataFormat.equals("AVRO")) {
        assert inputSchema != null : "To read from Kafka in AVRO format, you must provide a schema.";
      } else if (dataFormat != null && dataFormat.equals("PROTO")) {
        assert messageName != null
            : "To read from Kafka in PROTO format, messageName must be provided.";
        assert fileDescriptorPath != null || inputSchema != null
            : "To read from Kafka in PROTO format, fileDescriptorPath or schema must be provided.";
      } else {
        assert inputSchema != null : "To read from Kafka in JSON format, you must provide a schema.";
      }
    }

    public static Configuration.Builder builder() {
      return new AutoValue_JavascriptRowTransformerProvider_Configuration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setJavascriptTextTransformGcsPath(String path);

      public abstract Builder setJavascriptTextTransformFunctionName(String name);

      public abstract Builder setJavascriptTextTransformReloadIntervalMinutes(Integer minutes);

      public abstract Builder setLoggingEnabled(Boolean enabled);

      public abstract String setFormat();

      public abstract String setSchema();

      public abstract String setFileDescriptorPath();

      public abstract String setMessageName();

      public abstract Builder setErrorHandling(ErrorHandling errorHandling);

      public abstract Configuration build();
    }
  }

  protected static class JavascriptRowTransform extends SchemaTransform {

    private static final Logger LOG = LoggerFactory.getLogger(JavascriptRowTransform.class);

    private final JavascriptRowTransformerProvider.Configuration configuration;

    private static final TupleTag<Row> successValues = new TupleTag<>() {
    };
    private static final TupleTag<Row> errorValues = new TupleTag<>() {
    };

    private Counter successCounter =
        Metrics.counter(JavascriptRowTransformerProvider.JavascriptRowTransform.class, "udf-transform-success-count");

    private Counter failedCounter =
        Metrics.counter(JavascriptRowTransformerProvider.JavascriptRowTransform.class, "udf-transform-failed-count");

    public JavascriptRowTransform(JavascriptRowTransformerProvider.Configuration configuration) {
      this.configuration = configuration;
    }

    private DoFn<Row, Row> createDoFn(String fileSystemPath, String functionName, Integer reloadIntervalMinutes, Schema errorSchema, Boolean loggingEnabled, SerializableFunction<byte[], Row> valueMapper) {
      return new DoFn<Row, Row>() {
        private JavascriptTextTransformer.JavascriptRuntime javascriptRuntime;

        @Setup
        public void setup() {
          if (fileSystemPath != null && functionName != null) {
            javascriptRuntime =
                getJavascriptRuntime(
                    fileSystemPath,
                    functionName,
                    reloadIntervalMinutes);
          }
        }

        private void handleException(String message, Throwable e, MultiOutputReceiver out, Row inputRow) {
          boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());
          if (loggingEnabled) {
            LOG.warn(
                message,
                functionName,
                fileSystemPath,
                e.getMessage());
          }
          if (handleErrors) {
            out.get(errorValues).output(ErrorHandling.errorRecord(errorSchema, inputRow, e));
            failedCounter.inc();
          } else {
            throw new RuntimeException(e);
          }
        }

        @ProcessElement
        public void processElement(@Element Row inputRow, MultiOutputReceiver out) {
          Row element = inputRow;

          try {
            if (javascriptRuntime != null) {
              String json = javascriptRuntime.invoke(inputRow.getString(PAYLOAD_TAG));
              try {
                element = valueMapper.apply(json.getBytes());
              } catch (Exception e) {
                handleException("Error while parsing the element", e, out, inputRow);
              }
            }
            if (element != null) {
              out.get(successValues).output(element);
              successCounter.inc();
            }
          } catch (ScriptException | IOException | NoSuchMethodException e) {
            handleException("Exception occurred while applying UDF '{}' from file path '{}' due"
                + " to '{}'", e, out, inputRow);
          } catch (Throwable e) {
            handleException("Unexpected error occurred while applying UDF '{}' from file path '{}' due"
                + " to '{}'", e, out, inputRow);
          }
        }
      };
    }

    private SerializableFunction<byte[], Row> getRawBytesToRowFunction(Schema rawSchema) {
      return new SimpleFunction<>() {
        @Override
        public Row apply(byte[] input) {
          return Row.withSchema(rawSchema).addValue(input).build();
        }
      };
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {

      String fileSystemPath = configuration.getJavascriptTextTransformGcsPath();
      String functionName = configuration.getJavascriptTextTransformFunctionName();
      Integer reloadIntervalMinutes = configuration.getJavascriptTextTransformReloadIntervalMinutes();
      Boolean loggingEnabled = configuration.getLoggingEnabled() != null && configuration.getLoggingEnabled();

      Schema inputSchema = input.get(INPUT_ROWS_TAG).getSchema();
      Schema errorSchema = ErrorHandling.errorSchema(inputSchema);
      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());

      String format = Objects.requireNonNull(configuration.getFormat());
      String schema = Objects.requireNonNull(configuration.getSchema());

      Schema outputSchema;
      SerializableFunction<byte[], Row> valueMapper;

      switch (format) {
        case "RAW":
          outputSchema = Schema.builder().addField(PAYLOAD_TAG, Schema.FieldType.BYTES).build();
          valueMapper = getRawBytesToRowFunction(outputSchema);
          break;
        case "PROTO":
          String fileDescriptorPath = configuration.getFileDescriptorPath();
          String messageName = Objects.requireNonNull(configuration.getMessageName());
          if (fileDescriptorPath != null) {
            outputSchema = ProtoByteUtils.getBeamSchemaFromProto(fileDescriptorPath, messageName);
            valueMapper = ProtoByteUtils.getProtoBytesToRowFunction(fileDescriptorPath, messageName);
          } else {
            outputSchema = ProtoByteUtils.getBeamSchemaFromProtoSchema(schema, messageName);
            valueMapper = ProtoByteUtils.getProtoBytesToRowFromSchemaFunction(schema, messageName);
          }
          break;
        case "AVRO":
          outputSchema = AvroUtils.toBeamSchema(new org.apache.avro.Schema.Parser().parse(schema));
          valueMapper = AvroUtils.getAvroBytesToRowFunction(outputSchema);
          break;
        default:
          outputSchema = JsonUtils.beamSchemaFromJsonSchema(schema);
          valueMapper = JsonUtils.getJsonBytesToRowFunction(outputSchema);
          break;
      }

      PCollectionTuple pcolls =
          input
              .get(INPUT_ROWS_TAG)
              .apply(
                  "ProcessUdf",
                  ParDo.of(createDoFn(fileSystemPath, functionName, reloadIntervalMinutes, errorSchema, loggingEnabled, valueMapper))
                      .withOutputTags(successValues, TupleTagList.of(errorValues)));
      pcolls.get(successValues).setRowSchema(outputSchema);
      pcolls.get(errorValues).setRowSchema(errorSchema);

      PCollectionRowTuple result =
          PCollectionRowTuple.of(OUTPUT_ROWS_TAG, pcolls.get(successValues));
      if (handleErrors) {
        result = result.and(configuration.getErrorHandling().getOutput(), pcolls.get(errorValues));
      }
      return result;
    }
  }
}
