/*
 * Copyright (C) 2020 Google LLC
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

import com.google.api.services.bigquery.model.TableRow;
import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.transforms.BigtableConverters.FailsafeJsonToMutation;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.v2.transforms.PythonTextTransformer.FailsafePythonUdf;
import com.google.cloud.teleport.v2.transforms.PythonTextTransformer.PythonTextTransformerOptions;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * The {@link UDFTextTransformer} class is a {@link PTransform} which transforms incoming objects into {@link TableRow} objects for insertion into BigQuery while applying an
 * optional UDF to the input. The executions of the UDF and transformation to {@link TableRow}
 * objects is done in a fail-safe way by wrapping the element with its original payload inside the
 * PCollectionTuple} which contains all output and dead-letter {@link PCollection}.
 *
 * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
 *
 */
public class UDFTextTransformer {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(UDFTextTransformer.class);

  /** Generic pipeline options for sundry text transformers. */
  public interface InputUDFOptions
      extends PipelineOptions, JavascriptTextTransformerOptions, PythonTextTransformerOptions {}

  /**
   * Primary class for taking a generic input, applying a text transform, and converting to a
   * tableRow.
   */
  public static class InputUDFToTableRow<InputT>
      extends PTransform<PCollection<FailsafeElement<InputT, String>>, PCollectionTuple> {

    /** The tag for the main output of the json transformation. */
    public TupleTag<TableRow> transformOut = new TupleTag<TableRow>() {};

    /** The tag for the main output for the UDF. */
    public TupleTag<FailsafeElement<InputT, String>> udfTempOut =
        new TupleTag<FailsafeElement<InputT, String>>() {};

    /** The tag for the dead-letter output of the udf. */
    public TupleTag<FailsafeElement<InputT, String>> udfDeadletterOut =
        new TupleTag<FailsafeElement<InputT, String>>() {};

    /** The tag for the dead-letter output of the json to table row transform. */
    public TupleTag<FailsafeElement<InputT, String>> transformDeadletterOut =
        new TupleTag<FailsafeElement<InputT, String>>() {};

    // public InputUDFOptions options;
    public String javascriptTransformPath;
    public String javascriptTransformFnName;
    public String pythonTransformPath;
    public String pythonTransformFnName;
    public Integer pythonTransformRetries;

    private FailsafeElementCoder<InputT, String> coder;

    /**
     * Primary entrypoint for the UDFTextTransformer. The method will accept a JSON string and send
     * it to the python transformer or JavaScript transformer depending on the pipeline options
     * provided.
     */
    public InputUDFToTableRow(
        String javascriptTransformPath,
        String javascriptTransformFnName,
        String pythonTransformPath,
        String pythonTransformFnName,
        Integer pythonTransformRetries,
        FailsafeElementCoder<InputT, String> coder) {
      this.javascriptTransformPath = javascriptTransformPath;
      this.javascriptTransformFnName = javascriptTransformFnName;
      this.pythonTransformPath = pythonTransformPath;
      this.pythonTransformFnName = pythonTransformFnName;
      this.pythonTransformRetries = pythonTransformRetries;
      this.coder = coder;
    }

    public PCollectionTuple expand(PCollection<FailsafeElement<InputT, String>> input) {

      PCollectionTuple udfOut;

      if (this.pythonTransformPath != null) {
        udfOut =
            input.apply(
                "InvokeUDF",
                FailsafePythonUdf.<InputT>newBuilder()
                    .setFileSystemPath(this.pythonTransformPath)
                    .setFunctionName(this.pythonTransformFnName)
                    .setRuntimeVersion("python3")
                    .setRuntimeRetries(this.pythonTransformRetries)
                    .setSuccessTag(udfTempOut)
                    .setFailureTag(udfDeadletterOut)
                    .build());
      } else {
        udfOut =
            input.apply(
                "InvokeUDF",
                FailsafeJavascriptUdf.<InputT>newBuilder()
                    .setFileSystemPath(this.javascriptTransformPath)
                    .setFunctionName(this.javascriptTransformFnName)
                    .setSuccessTag(udfTempOut)
                    .setFailureTag(udfDeadletterOut)
                    .build());
      }
      udfOut.get(udfTempOut).setCoder(this.coder);
      udfOut.get(udfDeadletterOut).setCoder(this.coder);
      // Convert the records which were successfully processed by the UDF into TableRow objects.
      PCollectionTuple jsonToTableRowOut =
          udfOut
              .get(udfTempOut)
              .apply(
                  "JsonToTableRow",
                  FailsafeJsonToTableRow.<InputT>newBuilder()
                      .setSuccessTag(transformOut)
                      .setFailureTag(transformDeadletterOut)
                      .build());
      jsonToTableRowOut.get(transformDeadletterOut).setCoder(this.coder);

      // Re-wrap the PCollections so we can return a single PCollectionTuple
      return PCollectionTuple.of(transformOut, jsonToTableRowOut.get(transformOut))
          .and(udfDeadletterOut, udfOut.get(udfDeadletterOut))
          .and(transformDeadletterOut, jsonToTableRowOut.get(transformDeadletterOut));
    }
  }

  public static class InputUDFToMutation<InputT>
          extends PTransform<PCollection<FailsafeElement<InputT, String>>, PCollectionTuple> {

    /** The tag for the main output of the json transformation. */
    public TupleTag<KV<ByteString, Iterable<Mutation>>> transformOut = new TupleTag<KV<ByteString, Iterable<Mutation>>>() {};

    /** The tag for the main output for the UDF. */
    public TupleTag<FailsafeElement<InputT, String>> udfTempOut =
            new TupleTag<FailsafeElement<InputT, String>>() {};

    /** The tag for the dead-letter output of the udf. */
    public TupleTag<FailsafeElement<InputT, String>> udfDeadletterOut =
            new TupleTag<FailsafeElement<InputT, String>>() {};

    /** The tag for the dead-letter output of the json to table row transform. */
    public TupleTag<FailsafeElement<InputT, String>> transformDeadletterOut =
            new TupleTag<FailsafeElement<InputT, String>>() {};

    // public InputUDFOptions options;
    public Map<String, String> columnFamilies;
    public String defaultColumnFamily;
    public String javascriptTransformPath;
    public String javascriptTransformFnName;
    public String pythonTransformPath;
    public String pythonTransformFnName;
    public Integer pythonTransformRetries;

    private FailsafeElementCoder<InputT, String> coder;

    /**
     * Primary entrypoint for the UDFTextTransformer. The method will accept a JSON string and send
     * it to the python transformer or JavaScript transformer depending on the pipeline options
     * provided.
     */
    public InputUDFToMutation(
            Map<String, String> columnFamilies,
            String defaultColumnFamily,
            String javascriptTransformPath,
            String javascriptTransformFnName,
            String pythonTransformPath,
            String pythonTransformFnName,
            Integer pythonTransformRetries,
            FailsafeElementCoder<InputT, String> coder) {
      this.columnFamilies = columnFamilies;
      this.defaultColumnFamily = defaultColumnFamily;
      this.javascriptTransformPath = javascriptTransformPath;
      this.javascriptTransformFnName = javascriptTransformFnName;
      this.pythonTransformPath = pythonTransformPath;
      this.pythonTransformFnName = pythonTransformFnName;
      this.pythonTransformRetries = pythonTransformRetries;
      this.coder = coder;
    }

    public PCollectionTuple expand(PCollection<FailsafeElement<InputT, String>> input) {

      PCollectionTuple udfOut;

      if (this.pythonTransformPath != null) {
        udfOut =
                input.apply(
                        "InvokeUDF",
                        FailsafePythonUdf.<InputT>newBuilder()
                                .setFileSystemPath(this.pythonTransformPath)
                                .setFunctionName(this.pythonTransformFnName)
                                .setRuntimeVersion("python3")
                                .setRuntimeRetries(this.pythonTransformRetries)
                                .setSuccessTag(udfTempOut)
                                .setFailureTag(udfDeadletterOut)
                                .build());
      } else {
        udfOut =
                input.apply(
                        "InvokeUDF",
                        FailsafeJavascriptUdf.<InputT>newBuilder()
                                .setFileSystemPath(this.javascriptTransformPath)
                                .setFunctionName(this.javascriptTransformFnName)
                                .setSuccessTag(udfTempOut)
                                .setFailureTag(udfDeadletterOut)
                                .build());
      }
      udfOut.get(udfTempOut).setCoder(this.coder);
      udfOut.get(udfDeadletterOut).setCoder(this.coder);
      // Convert the records which were successfully processed by the UDF into TableRow objects.
      PCollectionTuple jsonToMutationOut =
              udfOut
                      .get(udfTempOut)
                      .apply(
                              "JsonToMutation",
                              FailsafeJsonToMutation.<InputT>newBuilder()
                                      .setSuccessTag(transformOut)
                                      .setFailureTag(transformDeadletterOut)
                                      .setColumnFamilies(this.columnFamilies)
                                      .setDefaultColumnFamily(this.defaultColumnFamily)
                                      .build());
      jsonToMutationOut.get(transformDeadletterOut).setCoder(this.coder);
      // Re-wrap the PCollections so we can return a single PCollectionTuple
      return PCollectionTuple.of(transformOut, jsonToMutationOut.get(transformOut))
              .and(udfDeadletterOut, udfOut.get(udfDeadletterOut))
              .and(transformDeadletterOut, jsonToMutationOut.get(transformDeadletterOut));
    }
  }
}
