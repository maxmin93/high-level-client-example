package com.example.aws.elasticsearch.demo.elasticgraph.model;

import lombok.Data;

@Data
public class ElasticEdgeDocument extends ElasticElementDocument {

    private String sid;     // id of out-vertex : source
    private String tid;     // id of in-vertex : target

}
