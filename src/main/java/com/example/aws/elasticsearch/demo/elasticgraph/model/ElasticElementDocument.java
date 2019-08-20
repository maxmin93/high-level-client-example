package com.example.aws.elasticsearch.demo.elasticgraph.model;

import lombok.Data;

import java.util.List;

@Data
public class ElasticElementDocument {

    private String id;
    private String label;
    private String datasource;
    private List<ElasticProperty> properties;

}
