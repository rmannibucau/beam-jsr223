package com.github.rmannibucau.beam.jsr223.java;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;

public class JavaEngineFactory implements ScriptEngineFactory {

    @Override
    public String getEngineName() {
        return "Java-Engine";
    }

    @Override
    public String getEngineVersion() {
        return "1.0";
    }

    @Override
    public List<String> getExtensions() {
        return emptyList();
    }

    @Override
    public List<String> getMimeTypes() {
        return singletonList("application/java");
    }

    @Override
    public List<String> getNames() {
        return singletonList("java");
    }

    @Override
    public String getLanguageName() {
        return "java";
    }

    @Override
    public String getLanguageVersion() {
        return System.getProperty("java.version", "8");
    }

    @Override
    public Object getParameter(final String key) {
        if (key.equals("javax.script.engine_version")) {
            return getEngineVersion();
        }
        if (key.equals("javax.script.engine")) {
            return getEngineName();
        }
        if (key.equals("javax.script.language")) {
            return getLanguageName();
        }
        if (key.equals("javax.script.language_version")) {
            return getLanguageVersion();
        }
        return null;
    }

    @Override
    public String getMethodCallSyntax(final String obj, final String m, final String... args) {
        return obj + "." + m + '(' + (args == null ? "" : Stream.of(args).collect(Collectors.joining(", "))) + ')';
    }

    @Override
    public String getOutputStatement(final String toDisplay) {
        return "System.out.println(" + toDisplay + ")";
    }

    @Override
    public String getProgram(final String... statements) {
        return Stream.of(statements).collect(Collectors.joining(";", "", ";"));
    }

    @Override
    public ScriptEngine getScriptEngine() {
        return new JavaEngine(this);
    }
}
