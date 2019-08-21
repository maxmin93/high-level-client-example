package com.example.aws.elasticsearch.demo.elasticgraph.model;

import lombok.Data;

import java.util.List;

@Data
public class ElasticElementDocument {

    protected String id;
    protected String label;
    protected String datasource;
    protected List<ElasticProperty> properties;

}
