package com.github.rmannibucau.beam.jsr223.engine;

public interface TypeAware {
    void set(final Class<?> from, final Class<?> to);
    void reset();
}
