package com.example.aws.elasticsearch.demo.elasticgraph;

import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticVertexDocument;
import com.example.aws.elasticsearch.demo.elasticgraph.repository.ElasticEdgeService;
import com.example.aws.elasticsearch.demo.elasticgraph.repository.ElasticGraphService;
import com.example.aws.elasticsearch.demo.elasticgraph.repository.ElasticVertexService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;

@Service
@Slf4j
public class ElasticGraphAPI implements BaseGraphAPI {

    private final static int DEFAULT_SIZE = 2500;

    private final RestHighLevelClient client;
    private final ObjectMapper mapper;

    private final ElasticVertexService vertices;
    private final ElasticEdgeService edges;
    private final ElasticGraphService graph;

    @Autowired
    public ElasticGraphAPI(
            RestHighLevelClient client,     // elasticsearch config
            ObjectMapper mapper             // spring boot web starter
    ) {
        this.client = client;
        this.mapper = mapper;

        this.vertices = new ElasticVertexService(client, mapper);
        this.edges = new ElasticEdgeService(client, mapper);
        this.graph = new ElasticGraphService(client, mapper);
    }

    @PostConstruct
    private void ready() throws Exception {
        graph.ready();      // if not exists index, create index
    }

    ///////////////////////////////////////////////////////////////

    public List<ElasticVertexDocument> findVerticesByDatasource(String datasource) throws Exception {
        return vertices.findByDatasource(DEFAULT_SIZE, datasource);
    }
    public List<ElasticVertexDocument> findVerticesByDatasource(int size, String datasource) throws Exception {
        return vertices.findByDatasource(size, datasource);
    }
}
