package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.transforms.PubSubToFailSafeElement;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.transforms.DoFn;

public class FailsafeToString extends DoFn<FailsafeElement, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
        FailsafeElement<String, String> message = context.element();
        context.output(message.getOriginalPayload());
    }

    public static PubSubToFailSafeElement create() {
        return new PubSubToFailSafeElement();
    }
}
