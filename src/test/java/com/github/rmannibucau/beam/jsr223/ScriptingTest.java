package com.github.rmannibucau.beam.jsr223;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class ScriptingTest {
    @Rule
    public volatile TestPipeline pipeline = TestPipeline.create();

    @Test
    public void run() {
        final PCollection<Integer> out = pipeline.apply(Create.of("v1", "v22"))
                                                   .apply(new Scripting<String, Integer>() {}
                                                           .withLanguage("js")
                                                           // .withScript("context.output(context.element().length());"))
                                                           .withScript("context.element().length();"));
        PAssert.that(out).containsInAnyOrder(2, 3);
        final PipelineResult result = pipeline.run();
        assertEquals(PipelineResult.State.DONE, result.waitUntilFinish());
    }
}
