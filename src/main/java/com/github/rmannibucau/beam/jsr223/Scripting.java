package com.github.rmannibucau.beam.jsr223;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public abstract class Scripting<A, B> extends PTransform<PCollection<A>,PCollection<B>> {
    private String language = "js";
    private String script = "";
    private Coder<B> coder;

    public static <F, T> Scripting<F, T> of(final Coder<T> coder) {
        final Scripting<F, T> scripting = new Scripting<F, T>() {};
        scripting.coder = coder;
        return scripting;
    }

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

    @Override // ensure we don't always need to set the coder
    public <T> Coder<T> getDefaultOutputCoder(final PCollection<A> input, final PCollection<T> output)
            throws CannotProvideCoderException {
        if (coder != null) {
            return (Coder<T>) coder;
        }
        final Type superclass = getClass().getGenericSuperclass();
        if (ParameterizedType.class.isInstance(superclass)) {
            final Type type = ParameterizedType.class.cast(superclass).getActualTypeArguments()[1];
            return (Coder<T>) output.getPipeline().getCoderRegistry().getCoder(TypeDescriptor.of(type));
        }
        return (Coder<T>) SerializableCoder.of(Serializable.class);
    }

    private static class ScriptingFn<A, B> extends DoFn<A, B> {
        private String language;
        private String script;

        private volatile ScriptEngine engine;
        private CompiledScript compiledScript;

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
            if (Compilable.class.isInstance(engine)) {
                try {
                    compiledScript = Compilable.class.cast(engine).compile(script);
                } catch (ScriptException e) {
                    throw new IllegalStateException(e);
                }
            } else {
                compiledScript = new CompiledScript() {
                    @Override
                    public Object eval(final ScriptContext context) throws ScriptException {
                        return engine.eval(script, context);
                    }

                    @Override
                    public ScriptEngine getEngine() {
                        return engine;
                    }
                };
            }
        }

        @ProcessElement
        public void onElement(final ProcessContext context) {
            final Bindings bindings = engine.createBindings();
            bindings.put("context", context);

            final SimpleScriptContext scriptContext = new SimpleScriptContext();
            scriptContext.setBindings(bindings, ScriptContext.ENGINE_SCOPE);

            try {
                final Object eval = compiledScript.eval(scriptContext);
                if (eval != null) { // if the script returns sthg it is the output otherwise assume it uses context.output()
                    context.output((B) eval);
                }
            } catch (final ScriptException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
