package com.example.aws.elasticsearch.demo.elasticgraph;

import com.example.aws.elasticsearch.demo.basegraph.BaseGraphAPI;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticEdge;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticVertex;
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

    public long countV() throws Exception { return vertices.count(); }
    public long countV(String datasource) throws Exception { return vertices.count(datasource); }

    public long countE() throws Exception { return edges.count(); }
    public long countE(String datasource) throws Exception { return edges.count(datasource); }

    ///////////////////////////////////////////////////////////////
    // add, update, remove, find

    public String addVertex(ElasticVertex document) throws Exception {
        return vertices.createDocument(document);
    }
    public String updateVertex(ElasticVertex document) throws Exception {
        return vertices.updateDocument(document);
    }
    public String removeVertex(String id) throws Exception {
        return vertices.deleteDocument(id);
    }
    public ElasticVertex findVertex(String id) throws Exception {
        return vertices.findById(id);
    }

    public String addEdge(ElasticEdge document) throws Exception {
        return edges.createDocument(document);
    }
    public String updateEdge(ElasticEdge document) throws Exception {
        return edges.updateDocument(document);
    }
    public String removeEdge(String id) throws Exception {
        return edges.deleteDocument(id);
    }
    public ElasticEdge findEdge(String id) throws Exception {
        return edges.findById(id);
    }

    ///////////////////////////////////////////////////////////////
    // find vertices

    public List<ElasticVertex> findVerticesByDatasource(String datasource) throws Exception {
        return vertices.findByDatasource(DEFAULT_SIZE, datasource);
    }
    public List<ElasticVertex> findVerticesByDatasource(int size, String datasource) throws Exception {
        return vertices.findByDatasource(size, datasource);
    }

    public List<ElasticVertex> findVerticesByDatasourceAndLabel(String datasource, String label) throws Exception {
        return vertices.findByDatasourceAndLabel(DEFAULT_SIZE, datasource, label);
    }
    public List<ElasticVertex> findVerticesByDatasourceAndLabel(int size, String datasource, String label) throws Exception {
        return vertices.findByDatasourceAndLabel(size, datasource, label);
    }

    ///////////////////////////////////////////////////////////////
    // find edges

    public List<ElasticEdge> findEdgesByDatasource(String datasource) throws Exception {
        return edges.findByDatasource(DEFAULT_SIZE, datasource);
    }
    public List<ElasticEdge> findEdgesByDatasource(int size, String datasource) throws Exception {
        return edges.findByDatasource(size, datasource);
    }

    public List<ElasticEdge> findEdgesByDatasourceAndLabel(String datasource, String label) throws Exception {
        return edges.findByDatasourceAndLabel(DEFAULT_SIZE, datasource, label);
    }
    public List<ElasticEdge> findEdgesByDatasourceAndLabel(int size, String datasource, String label) throws Exception {
        return edges.findByDatasourceAndLabel(size, datasource, label);
    }
}
