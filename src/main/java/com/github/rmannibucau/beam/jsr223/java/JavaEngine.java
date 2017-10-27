package com.github.rmannibucau.beam.jsr223.java;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import com.github.rmannibucau.beam.jsr223.engine.TypeAware;

public class JavaEngine extends AbstractScriptEngine implements Compilable, TypeAware {
    private final ScriptEngineFactory factory;

    private Class<?> from;
    private Class<?> to;

    public JavaEngine(final ScriptEngineFactory factory) {
        this.factory = factory;
    }

    @Override
    public CompiledScript compile(final String script) throws ScriptException {
        final JavaCompiler compiler = requireNonNull(ToolProvider.getSystemJavaCompiler(), "you must run on a JDK to have a compiler");
        Path tmpDir = null;
        try {
            tmpDir = Files.createTempDirectory(getClass().getSimpleName());

            // todo: maybe make it configurable through the engine to ensure it is stable from outside the engine?
            final String packageName = "com.github.rmannibucau.beam.jsr223.java.generated";
            final String className = "JavaCompiledScript_" + UUID.randomUUID().toString().replace("-", "");
            final String source = toSource(packageName, className, script);
            final File src = new File(tmpDir.toFile(), "sources/");
            final File bin = new File(tmpDir.toFile(), "bin/");
            final File srcDir = new File(src, packageName.replace('.', '/'));
            srcDir.mkdirs();
            bin.mkdirs();
            final File java = new File(srcDir, className + ".java");
            try (final Writer writer = new FileWriter(java)) {
                writer.write(source);
            }

            final String classpath = System.getProperty(getClass().getName() + ".classpath",
                            System.getProperty("java.class.path", System.getProperty("surefire.real.class.path")));
            final Collection<String> args = Stream.of(
                    "-classpath", classpath,
                    "-sourcepath", src.getAbsolutePath(),
                    "-d", bin.getAbsolutePath(),
                    java.getAbsolutePath()).collect(toList());

            final int run = compiler.run(null, System.out, System.err, args.toArray(new String[args.size()]));
            if (run != 0) {
                throw new IllegalArgumentException("Can't compile the incoming script, here is the generated code: >\n" + source + "\n<\n");
            }
            final URLClassLoader loader = new URLClassLoader(new URL[]{bin.toURI().toURL()});
            final Class<? extends CompiledScript> loadClass = (Class<? extends CompiledScript>)
                    loader.loadClass(packageName + '.' + className);
            return loadClass.getConstructor(ScriptEngine.class, URLClassLoader.class).newInstance(this, loader);
        } catch (final Exception e) {
            throw new IllegalArgumentException(e);
        } finally {
            if (tmpDir != null) {
                try {
                    Files.walkFileTree(tmpDir, new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                            Files.delete(file);
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                            Files.delete(dir);
                            return FileVisitResult.CONTINUE;
                        }

                    });
                } catch (final IOException e) {
                    // no-op: todo: throw an exception if not already thrown or add a suppressed
                }
            }
        }
    }

    private String toSource(final String pck, final String name, final String script) {
        return "package " + pck + ";\n\n" +
                "import java.io.*;\n" +
                "import java.net.*;\n" +
                "import " + from.getName() + ";\n" +
                "import " + to.getName() + ";\n" +
                "import org.apache.beam.sdk.transforms.DoFn;\n" +
                "import javax.script.ScriptContext;\n" +
                "import javax.script.ScriptEngine;\n" +
                "import javax.script.ScriptException;\n" +
                "import javax.script.CompiledScript;\n" +
                "import java.net.URLClassLoader;\n\n" +
                "public class " + name + " extends CompiledScript implements AutoCloseable {\n" +
                "    private final ScriptEngine $engine;\n" +
                "    private final URLClassLoader $loader;\n" +
                "\n" +
                "    public " + name + "(final ScriptEngine engine, final URLClassLoader loader) {\n" +
                "        this.$engine = engine;\n" +
                "        this.$loader = loader;\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    public Object eval(final ScriptContext $context) throws ScriptException {\n" +
                "        try {\n" +
                "           final org.apache.beam.sdk.transforms.DoFn<" + from.getName() + "," + to.getName() + ">.ProcessContext context = \n" +
                "               DoFn.ProcessContext.class.cast($context.getBindings(ScriptContext.ENGINE_SCOPE).get(\"context\"));\n" +
                "           " + script + "\n" +
                "           return null;\n" + // assume the script doesn't return anything for now
                "        } catch (final Exception e) {\n" +
                "            if (RuntimeException.class.isInstance(e)) {\n" +
                "                throw RuntimeException.class.cast(e);\n" +
                "            }\n" +
                "            throw new IllegalStateException(e);\n" +
                "        }\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    public ScriptEngine getEngine() {\n" +
                "        return $engine;\n" +
                "    }\n" +
                "\n" +
                "    @Override\n" +
                "    public void close() throws Exception {\n" +
                "        $loader.close();\n" +
                "    }\n" +
                "}";
    }

    @Override
    public Object eval(final String script, final ScriptContext context) throws ScriptException {
        return compile(script).eval(context);
    }

    @Override
    public Object eval(final Reader reader, final ScriptContext context) throws ScriptException {
        return eval(load(reader), context);
    }

    @Override
    public CompiledScript compile(final Reader script) throws ScriptException {
        return compile(load(script));
    }

    @Override
    public Bindings createBindings() {
        return new SimpleBindings();
    }

    @Override
    public ScriptEngineFactory getFactory() {
        return factory;
    }

    private String load(final Reader reader) {
        return new BufferedReader(reader).lines().collect(Collectors.joining("\n"));
    }

    @Override
    public void set(final Class<?> from, final Class<?> to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public void reset() {
        this.from = this.to = null;
    }
}
