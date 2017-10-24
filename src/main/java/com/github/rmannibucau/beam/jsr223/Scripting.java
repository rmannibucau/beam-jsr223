package com.github.rmannibucau.beam.jsr223;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class Scripting<A, B> extends PTransform<PCollection<A>,PCollection<B>> {
    private String language = "js";
    private String script = "";

    public Scripting<A, B> withLanguage(final String language) {
        this.language = language;
        return this;
    }

    public Scripting<A, B> withScript(final String script) {
        this.script = script;
        return this;
    }

    @Override
    public PCollection<B> expand(final PCollection<A> apCollection) {
        if (language == null || script == null || script.isEmpty()) {
            throw new IllegalArgumentException("Language and Script must be set");
        }
        return apCollection.apply(ParDo.of(new ScriptingFn<>(language, script)));
    }

    private static class ScriptingFn<A, B> extends DoFn<A, B> {
        private String language;
        private String script;

        private volatile ScriptEngine engine;

        ScriptingFn(final String language, final String script) {
            this.language = language;
            this.script = script;
        }

        @Setup
        public void onSetup() {
            final ScriptEngineManager manager = new ScriptEngineManager(Thread.currentThread().getContextClassLoader());
            engine = manager.getEngineByExtension(language);
            if (engine == null) {
                engine = manager.getEngineByName(language);
                if (engine == null) {
                    engine = manager.getEngineByMimeType(language);
                }
            }
        }

        @ProcessElement
        public void onElement(final ProcessContext context) {
            final Bindings bindings = engine.createBindings();
            bindings.put("context", context);
            try {
                final Object eval = engine.eval(script, bindings);
                if (eval != null) { // if the script returns sthg it is the output otherwise assume it uses context.output()
                    context.output((B) eval);
                }
            } catch (final ScriptException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}