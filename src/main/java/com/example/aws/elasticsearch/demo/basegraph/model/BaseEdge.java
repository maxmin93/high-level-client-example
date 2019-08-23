package com.example.aws.elasticsearch.demo.basegraph.model;

public interface BaseEdge extends BaseElement {

    public static final String DEFAULT_LABEL = "edge";

    String getSid();
    String getTid();

}
