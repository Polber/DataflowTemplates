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

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.auto.Consumes;
import com.google.cloud.teleport.metadata.auto.Outputs;
import com.google.cloud.teleport.metadata.auto.TemplateTransform;
import com.google.cloud.teleport.v2.auto.blocks.WriteToPubSub.WriteToPubSubOptions;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.values.PCollection;

public class WriteToPubSub implements TemplateTransform<WriteToPubSubOptions> {
  public interface WriteToPubSubOptions extends PipelineOptions {
    @TemplateParameter.PubsubTopic(
        order = 8,
        groupName = "Target",
        description = "Output Pub/Sub topic",
        helpText = "The name of the topic to publish data to.",
        example = "projects/<PROJECT_ID>/topics/<TOPIC_NAME>")
    @Validation.Required
    String getOutputTopic();

    void setOutputTopic(String outputTopic);
  }

  @Consumes(String.class)
  @Outputs(
      value = FailsafeElement.class,
      types = {String.class, String.class})
  public void writeJson(PCollection<String> input, WriteToPubSubOptions options) {
    input.apply("writeSuccessMessages", PubsubIO.writeStrings().to(options.getOutputTopic()));
  }

  @Override
  public Class<WriteToPubSubOptions> getOptionsClass() {
    return WriteToPubSubOptions.class;
  }
}
