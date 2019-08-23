package com.example.aws.elasticsearch.demo.basegraph.model;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.NoSuchElementException;

public interface BaseProperty {

    String key();
    String type();
    Object value() throws NoSuchElementException;

    default boolean isPresent() {
        try{
            Object value = this.value();
            return value != null;
        }catch (NoSuchElementException ex){
            return false;
        }
    }

    static BaseProperty empty() {
        return BaseEmptyProperty.instance();
    }

    final class BaseEmptyProperty implements BaseProperty {

        private static final BaseEmptyProperty INSTANCE = new BaseEmptyProperty();
        private static final String EMPTY_PROPERTY = "p[empty]";

        private BaseEmptyProperty() {
        }

        @Override
        public String key() {
            throw new NoSuchElementException("BaseEmptyProperty");
        }
        @Override
        public String type() throws NoSuchElementException {
            throw new NoSuchElementException("BaseEmptyProperty");
        }
        @Override
        public String value() throws NoSuchElementException {
            throw new NoSuchElementException("BaseEmptyProperty");
        }
        @Override
        public String toString() {
            return EMPTY_PROPERTY;
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public boolean equals(final Object object) {
            return object instanceof BaseEmptyProperty;
        }

        public int hashCode() {
            return 123456789;
        }

        public static BaseProperty instance() {
            return INSTANCE;
        }
    }
}
