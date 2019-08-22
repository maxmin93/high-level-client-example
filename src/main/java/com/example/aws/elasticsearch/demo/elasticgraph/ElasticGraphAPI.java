package com.example.aws.elasticsearch.demo.elasticgraph;

import com.example.aws.elasticsearch.demo.basegraph.BaseGraphAPI;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticEdge;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticVertex;
import com.example.aws.elasticsearch.demo.elasticgraph.repository.ElasticEdgeService;
import com.example.aws.elasticsearch.demo.elasticgraph.repository.ElasticGraphService;
import com.example.aws.elasticsearch.demo.elasticgraph.repository.ElasticVertexService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
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

    public String count() throws Exception {
        Gson gson = new Gson();
        JsonObject object = new JsonObject();
        object.addProperty("V", vertices.count());
        object.addProperty("E", edges.count());
        return gson.toJson(object);
    }
    public String count(String datasource) throws Exception {
        Gson gson = new Gson();
        JsonObject object = new JsonObject();
        object.addProperty("V", vertices.count(datasource));
        object.addProperty("E", edges.count(datasource));
        return gson.toJson(object);
    }

    public String remove(String datasource) throws Exception {
        Gson gson = new Gson();
        JsonObject object = new JsonObject();
        object.addProperty("V", vertices.deleteDocuments(datasource));
        object.addProperty("E", edges.deleteDocuments(datasource));
        return gson.toJson(object);
    }

    public long countV() throws Exception { return vertices.count(); }
    public long countV(String datasource) throws Exception { return vertices.count(datasource); }

    public long countE() throws Exception { return edges.count(); }
    public long countE(String datasource) throws Exception { return edges.count(datasource); }


    ///////////////////////////////////////////////////////////////
    // add, update, remove, find

    public String addV(ElasticVertex document) throws Exception {
        return vertices.createDocument(document);
    }
    public String updateV(ElasticVertex document) throws Exception {
        return vertices.updateDocument(document);
    }
    public String removeV(String id) throws Exception {
        return vertices.deleteDocument(id);
    }
    public ElasticVertex findV(String id) throws Exception {
        return vertices.findById(id);
    }

    public String addE(ElasticEdge document) throws Exception {
        return edges.createDocument(document);
    }
    public String updateE(ElasticEdge document) throws Exception {
        return edges.updateDocument(document);
    }
    public String removeE(String id) throws Exception {
        return edges.deleteDocument(id);
    }
    public ElasticEdge findE(String id) throws Exception {
        return edges.findById(id);
    }

    ///////////////////////////////////////////////////////////////
    //  **NOTE: optimized find functions
    //      ==> http://tinkerpop.apache.org/docs/current/reference/#has-step
    //    has(key,value)          AND : findVerticesWithKV(ds, key, val)
    //    has(label, key, value)  AND : findVerticesWithLKV(ds, label, key, value)
    //    hasLabel(labels…​)       OR  : findVerticesWithLabels(ds, ...labels)
    //    hasId(ids…​)             OR  : findVertices(ids)
    //    hasKey(keys…​)           AND : findVerticesWithKeys(ds, ...keys)
    //    hasValue(values…​)       AND : findVerticesWithValues(ds, ...values)
    //    has(key)                EQ  : findVerticesWithKey(ds, key)
    //    hasNot(key)             NEQ : findVerticesWithNotKey(ds, key)

    ///////////////////////////////////////////////////////////////
    // find vertices

    // V.hasId : ids
    public List<ElasticVertex> findV_IDs(String[] ids) throws Exception {
        return vertices.findByIDs(ids);
    }

    // V : datasource
    public List<ElasticVertex> findV_Datasource(String datasource) throws Exception {
        return vertices.findByDatasource(DEFAULT_SIZE, datasource);
    }
    public List<ElasticVertex> findV_Datasource(int size, String datasource) throws Exception {
        return vertices.findByDatasource(size, datasource);
    }

    // V : datasource + labels
    public List<ElasticVertex> findV_DatasourceAndLabels(String datasource, String[] labels) throws Exception {
        return vertices.findByDatasourceAndLabels(DEFAULT_SIZE, datasource, labels);
    }
    public List<ElasticVertex> findV_DatasourceAndLabels(int size, String datasource, String[] labels) throws Exception {
        return vertices.findByDatasourceAndLabels(size, datasource, labels);
    }

    // V : datasource + property.key
    public List<ElasticVertex> findV_DatasourceAndPropertyKey(String datasource, String key) throws Exception {
        return vertices.findByDatasourceAndPropertyKey(DEFAULT_SIZE, datasource, key);
    }
    public List<ElasticVertex> findV_DatasourceAndPropertyKey(int size, String datasource, String key) throws Exception {
        return vertices.findByDatasourceAndPropertyKey(size, datasource, key);
    }

    // V : datasource + property.value
    public List<ElasticVertex> findV_DatasourceAndPropertyValue(String datasource, String value) throws Exception {
        return vertices.findByDatasourceAndPropertyValue(DEFAULT_SIZE, datasource, value);
    }
    public List<ElasticVertex> findV_DatasourceAndPropertyValue(int size, String datasource, String value) throws Exception {
        return vertices.findByDatasourceAndPropertyValue(size, datasource, value);
    }
    public List<ElasticVertex> findV_DatasourceAndPropertyValuePartial(String datasource, String value) throws Exception {
        return vertices.findByDatasourceAndPropertyValuePartial(DEFAULT_SIZE, datasource, value);
    }

    // V : datasource + (property.key & .value)
    public List<ElasticVertex> findV_DatasourceAndPropertyKeyValue(String datasource, String key, String value) throws Exception {
        return vertices.findByDatasourceAndPropertyKeyValue(DEFAULT_SIZE, datasource, key, value);
    }
    public List<ElasticVertex> findV_DatasourceAndPropertyKeyValue(int size, String datasource, String key, String value) throws Exception {
        return vertices.findByDatasourceAndPropertyKeyValue(size, datasource, key, value);
    }

    // V.has(label,key,value) : datasource + label + (property.key & .value)
    public List<ElasticVertex> findV_DatasourceAndLabelAndPropertyKeyValue(String datasource, String label, String key, String value) throws Exception {
        return vertices.findByDatasourceAndLabelAndPropertyKeyValue(DEFAULT_SIZE, datasource, label, key, value);
    }
    public List<ElasticVertex> findV_DatasourceAndLabelAndPropertyKeyValue(int size, String datasource, String label, String key, String value) throws Exception {
        return vertices.findByDatasourceAndLabelAndPropertyKeyValue(size, datasource, label, key, value);
    }

    ///////////////////////////////////////////////////////////////
    // find edges

    // E.hasId : ids
    public List<ElasticEdge> findE_IDs(String[] ids) throws Exception {
        return edges.findByIDs(ids);
    }

    // E : datasource
    public List<ElasticEdge> findE_Datasource(String datasource) throws Exception {
        return edges.findByDatasource(DEFAULT_SIZE, datasource);
    }
    public List<ElasticEdge> findE_Datasource(int size, String datasource) throws Exception {
        return edges.findByDatasource(size, datasource);
    }

    // E.hasLabel : datasource + labels
    public List<ElasticEdge> findE_DatasourceAndLabels(String datasource, String[] labels) throws Exception {
        return edges.findByDatasourceAndLabels(DEFAULT_SIZE, datasource, labels);
    }
    public List<ElasticEdge> findE_DatasourceAndLabels(int size, String datasource, String[] labels) throws Exception {
        return edges.findByDatasourceAndLabels(size, datasource, labels);
    }

    // E : datasource + property.key
    public List<ElasticEdge> findE_DatasourceAndPropertyKey(String datasource, String key) throws Exception {
        return edges.findByDatasourceAndPropertyKey(DEFAULT_SIZE, datasource, key);
    }
    public List<ElasticEdge> findE_DatasourceAndPropertyKey(int size, String datasource, String key) throws Exception {
        return edges.findByDatasourceAndPropertyKey(size, datasource, key);
    }

    // E : datasource + property.value
    public List<ElasticEdge> findE_DatasourceAndPropertyValue(String datasource, String value) throws Exception {
        return edges.findByDatasourceAndPropertyValue(DEFAULT_SIZE, datasource, value);
    }
    public List<ElasticEdge> findE_DatasourceAndPropertyValue(int size, String datasource, String value) throws Exception {
        return edges.findByDatasourceAndPropertyValue(size, datasource, value);
    }
    public List<ElasticEdge> findE_DatasourceAndPropertyValuePartial(String datasource, String value) throws Exception {
        return edges.findByDatasourceAndPropertyValuePartial(DEFAULT_SIZE, datasource, value);
    }

    // E.has(key,value) : datasource + (property.key & .value)
    public List<ElasticEdge> findE_DatasourceAndPropertyKeyValue(String datasource, String key, String value) throws Exception {
        return edges.findByDatasourceAndPropertyKeyValue(DEFAULT_SIZE, datasource, key, value);
    }
    public List<ElasticEdge> findE_DatasourceAndPropertyKeyValue(int size, String datasource, String key, String value) throws Exception {
        return edges.findByDatasourceAndPropertyKeyValue(size, datasource, key, value);
    }

    // E.has(label,key,value) : datasource + label + (property.key & .value)
    public List<ElasticEdge> findE_DatasourceAndLabelAndPropertyKeyValue(String datasource, String label, String key, String value) throws Exception {
        return edges.findByDatasourceAndLabelAndPropertyKeyValue(DEFAULT_SIZE, datasource, label, key, value);
    }
    public List<ElasticEdge> findE_DatasourceAndLabelAndPropertyKeyValue(int size, String datasource, String label, String key, String value) throws Exception {
        return edges.findByDatasourceAndLabelAndPropertyKeyValue(size, datasource, label, key, value);
    }

}
