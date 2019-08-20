package com.example.aws.elasticsearch.demo.elasticgraph.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ElasticEdgeService {

    private RestHighLevelClient client;
    private ObjectMapper mapper;

    @Autowired
    public ElasticEdgeService(
            RestHighLevelClient client,     // elasticsearch config
            ObjectMapper mapper             // spring boot web starter
    ) {
        this.client = client;
        this.mapper = mapper;
    }

}
