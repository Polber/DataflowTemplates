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

import com.google.auto.service.AutoService;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.auto.Outputs;
import com.google.cloud.teleport.v2.auto.schema.TemplateOptionSchema;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.NonNull;

@AutoService(SchemaTransformProvider.class)
public class ReadFromPubSub extends TemplateReadTransform<ReadFromPubSub.ReadFromPubSubOptions> {

  @DefaultSchema(TemplateOptionSchema.class)
  public interface ReadFromPubSubOptions extends Configuration {

    @TemplateParameter.PubsubSubscription(
        order = 1,
        description = "Pub/Sub input subscription",
        helpText =
            "Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name'")
    @SchemaFieldDescription("Pub/Sub input subscription.")
    String getInputSubscription();

    void setInputSubscription(String input);
  }

  @Override
  public @NonNull String identifier() {
    return "blocks:external:org.apache.beam:read_from_pubsub:v1";
  }

//  @Outputs(
//      value = Row.class,
//      types = {RowTypes.PubSubMessageRow.class})
  @Outputs(PubsubMessage.class)
  public PCollectionTuple read(PBegin input, ReadFromPubSubOptions options) {
    return PCollectionTuple.of(
//        BlockConstants.OUTPUT_TAG,
        new TupleTag<PubsubMessage>(),
        input
            .apply(
                "ReadPubSubSubscription",
                PubsubIO.readMessagesWithAttributesAndMessageId()
                    .fromSubscription(options.getInputSubscription()))
//            .apply(
//                MapElements.into(TypeDescriptor.of(Row.class))
//                    .via(RowTypes.PubSubMessageRow::PubSubMessageToRow))
//            .setCoder(RowCoder.of(RowTypes.PubSubMessageRow.SCHEMA))
    );
  }

  @Override
  public Class<ReadFromPubSubOptions> getOptionsClass() {
    return ReadFromPubSubOptions.class;
  }
}
