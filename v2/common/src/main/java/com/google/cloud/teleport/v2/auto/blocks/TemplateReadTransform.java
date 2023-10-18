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

import com.google.cloud.teleport.metadata.auto.Outputs;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

public abstract class TemplateReadTransform<X extends PipelineOptions>
    extends TemplateTransformClass<X> {
  public abstract PCollectionTuple read(PBegin input, X config);

  public PCollectionRowTuple transform(PCollectionRowTuple input, X config) {
    return PCollectionRowTuple.of("", this.read(input.getPipeline().begin(), config).get(BlockConstants.OUTPUT_TAG));
  }

  @Override
  protected String getTransformName() {
    return "read";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }


  @Override
  public SchemaTransform from(X configuration) {
    return new SchemaTransform() {

      @Override
      public PCollectionRowTuple expand(PCollectionRowTuple input) {
        PCollectionTuple output = read(input.getPipeline().begin(), configuration);

        PCollection<Row> rows = output.get(BlockConstants.OUTPUT_TAG)
            .apply(getMappingFunction())
            .setCoder(getCoder());
//        return PCollectionRowTuple.of(OUTPUT_ROW_TAG, rows.setCoder(RowCoder.of(rows.getSchema())));
        return PCollectionRowTuple.of(OUTPUT_ROW_TAG, rows);
      }
    };
  }
}
