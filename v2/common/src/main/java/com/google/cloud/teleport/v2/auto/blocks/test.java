package com.google.cloud.teleport.v2.auto.blocks;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;

public class test {
  public static void main(String[] args) {

    ReadFromPubSub.ReadFromPubSubOptions readFromPubSubOptions = (ReadFromPubSub.ReadFromPubSubOptions) PipelineOptionsFactory.fromArgs("--inputSubscription=projects/cloud-teleport-testing/subscriptions/jkinard-yaml-test-sub")
        .withValidation()
        .as((Class<? extends PipelineOptions>) ReadFromPubSub.ReadFromPubSubOptions.class);

    PubsubMessageToTableRow.TransformOptions transformOptions = (PubsubMessageToTableRow.TransformOptions) PipelineOptionsFactory
        .as((Class<? extends PipelineOptions>) PubsubMessageToTableRow.TransformOptions.class);

    WriteToBigQuery.SinkOptions sinkOptions = (WriteToBigQuery.SinkOptions) PipelineOptionsFactory.fromArgs("--outputTableSpec=cloud-teleport-testing:jkinard_test.jkinard-yaml-table")
        .withValidation()
        .as((Class<? extends PipelineOptions>) WriteToBigQuery.SinkOptions.class);

    Pipeline pipeline = Pipeline.create(transformOptions);

    pipeline
        .apply(new PTransform<PBegin, PCollectionRowTuple>() {
          @Override
          public PCollectionRowTuple expand(PBegin input) {
            return PCollectionRowTuple.empty(input.getPipeline());
          }
        })
        .apply(new ReadFromPubSub().from(readFromPubSubOptions))
        .apply(new PubsubMessageToTableRow().from(transformOptions))
        .apply(new WriteToBigQuery().from(sinkOptions));

    pipeline.run();
  }

  public static class Nothing extends PTransform<PBegin, PCollectionRowTuple> {
    @Override
    public PCollectionRowTuple expand(PBegin input) {
      return PCollectionRowTuple.empty(input.getPipeline());
    }
  }
}
