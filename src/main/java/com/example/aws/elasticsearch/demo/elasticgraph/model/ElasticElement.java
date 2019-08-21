package com.example.aws.elasticsearch.demo.elasticgraph.model;

import com.example.aws.elasticsearch.demo.basegraph.model.BaseElement;
import lombok.Data;

import java.util.List;

@Data
public class ElasticElement implements BaseElement {

    protected String id;
    protected String label;
    protected String datasource;
    protected List<ElasticProperty> properties;

}
