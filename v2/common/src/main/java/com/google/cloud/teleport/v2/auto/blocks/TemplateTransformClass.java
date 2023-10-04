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

import com.google.cloud.teleport.metadata.auto.TemplateTransform;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;

public abstract class TemplateTransformClass<X extends PipelineOptions>
    extends TypedSchemaTransformProvider<X> implements TemplateTransform<X> {
  protected static final String INPUT_ROW_TAG = "input";
  protected static final String OUTPUT_ROW_TAG = "output";
  protected static final String ERROR_ROW_TAG = "errors";

  @Override
  public List<String> outputCollectionNames() {
    return List.of(OUTPUT_ROW_TAG, ERROR_ROW_TAG);
  }

  @Override
  public @NonNull Class<X> configurationClass() {
    return getOptionsClass();
  }

  public interface Configuration extends PipelineOptions {
    //    abstract void validate();
    //
    //    @SchemaFieldDescription("This option specifies whether and where to output unwritable
    // rows.")
    //    @Nullable
    //    TemplateTransformClass.Configuration.ErrorHandling getErrorHandling();
    //
    //    @AutoValue
    //    public abstract static class ErrorHandling {
    //      @SchemaFieldDescription("The name of the output PCollection containing failed writes.")
    //      public abstract String getOutput();
    //
    //      public static Configuration.ErrorHandling.Builder builder() {
    //        return new AutoValue_TemplateTransformClass_Configuration_ErrorHandling.Builder();
    //      }
    //
    //      @AutoValue.Builder
    //      public abstract static class Builder {
    //        public abstract Configuration.ErrorHandling.Builder setOutput(String output);
    //
    //        public abstract Configuration.ErrorHandling build();
    //      }
    //    }
    //
    //    public abstract static class Builder<T extends Builder<?>> {
    //      public abstract T setErrorHandling(
    //          TemplateTransformClass.Configuration.ErrorHandling errorHandling);
    //    }
  }

  public abstract PCollectionRowTuple transform(PCollectionRowTuple input, X config);

  @Override
  public SchemaTransform from(X configuration) {
    return new TemplateSchemaTransform(configuration);
  }

  private class TemplateSchemaTransform extends SchemaTransform {
    private final X configuration;

    private TemplateSchemaTransform(X configuration) {
      //      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> messages = input.get(INPUT_ROW_TAG);

      PCollectionRowTuple output =
          transform(PCollectionRowTuple.of(BlockConstants.OUTPUT_TAG, messages), configuration);

      PCollection<Row> rows = output.get(BlockConstants.OUTPUT_TAG);

      PCollectionRowTuple outputRowTuple = PCollectionRowTuple.of(OUTPUT_ROW_TAG, rows);
      //      if (configuration.getErrorHandling() != null) {
      //        PCollection<Row> errors = output.get(BlockConstants.ERROR_TAG);
      //        errors.setCoder(RowCoder.of(errors.getSchema()));
      //        outputRowTuple = outputRowTuple.and(configuration.getErrorHandling().getOutput(),
      // errors);
      //      }

      return outputRowTuple;
    }
  }

  @Override
  public List<String> inputCollectionNames() {
    return List.of(INPUT_ROW_TAG);
  }
}
