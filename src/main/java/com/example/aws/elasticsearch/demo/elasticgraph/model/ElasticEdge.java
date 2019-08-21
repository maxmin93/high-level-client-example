package com.example.aws.elasticsearch.demo.elasticgraph.model;

import com.example.aws.elasticsearch.demo.basegraph.model.BaseEdge;

import lombok.Data;

@Data
public class ElasticEdge extends ElasticElement implements BaseEdge {

    private String sid;     // id of out-vertex : source
    private String tid;     // id of in-vertex : target

}
