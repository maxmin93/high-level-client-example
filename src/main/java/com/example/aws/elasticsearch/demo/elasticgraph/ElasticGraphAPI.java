package com.example.aws.elasticsearch.demo.elasticgraph;

import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticEdgeDocument;
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

    public boolean reset() throws Exception {
        return graph.resetIndex();
    }

    ///////////////////////////////////////////////////////////////
    // add, update, remove, find

    public String addVertex(ElasticVertexDocument document) throws Exception {
        return vertices.createDocument(document);
    }
    public String updateVertex(ElasticVertexDocument document) throws Exception {
        return vertices.updateDocument(document);
    }
    public String removeVertex(String id) throws Exception {
        return vertices.deleteDocument(id);
    }
    public ElasticVertexDocument findVertex(String id) throws Exception {
        return vertices.findById(id);
    }

    public String addEdge(ElasticEdgeDocument document) throws Exception {
        return edges.createDocument(document);
    }
    public String updateEdge(ElasticEdgeDocument document) throws Exception {
        return edges.updateDocument(document);
    }
    public String removeEdge(String id) throws Exception {
        return edges.deleteDocument(id);
    }
    public ElasticEdgeDocument findEdge(String id) throws Exception {
        return edges.findById(id);
    }

    ///////////////////////////////////////////////////////////////
    // find vertices

    public List<ElasticVertexDocument> findVerticesByDatasource(String datasource) throws Exception {
        return vertices.findByDatasource(DEFAULT_SIZE, datasource);
    }
    public List<ElasticVertexDocument> findVerticesByDatasource(int size, String datasource) throws Exception {
        return vertices.findByDatasource(size, datasource);
    }

    public List<ElasticVertexDocument> findVerticesByDatasourceAndLabel(String datasource, String label) throws Exception {
        return vertices.findByDatasourceAndLabel(DEFAULT_SIZE, datasource, label);
    }
    public List<ElasticVertexDocument> findVerticesByDatasourceAndLabel(int size, String datasource, String label) throws Exception {
        return vertices.findByDatasourceAndLabel(size, datasource, label);
    }

    ///////////////////////////////////////////////////////////////
    // find edges

    public List<ElasticEdgeDocument> findEdgesByDatasource(String datasource) throws Exception {
        return edges.findByDatasource(DEFAULT_SIZE, datasource);
    }
    public List<ElasticEdgeDocument> findEdgesByDatasource(int size, String datasource) throws Exception {
        return edges.findByDatasource(size, datasource);
    }

    public List<ElasticEdgeDocument> findEdgesByDatasourceAndLabel(String datasource, String label) throws Exception {
        return edges.findByDatasourceAndLabel(DEFAULT_SIZE, datasource, label);
    }
    public List<ElasticEdgeDocument> findEdgesByDatasourceAndLabel(int size, String datasource, String label) throws Exception {
        return edges.findByDatasourceAndLabel(size, datasource, label);
    }
}
