package com.example.aws.elasticsearch.demo.elasticgraph.repository;

import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticVertex;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;
import java.util.Map;

@Slf4j
public class ElasticVertexService extends ElasticElementService {

    private final String INDEX;

    public ElasticVertexService(
            RestHighLevelClient client,     // elasticsearch config
            ObjectMapper mapper             // spring boot web starter
    ) {
        super(client, mapper);
        this.INDEX = ElasticGraphService.INDEX_VERTEX;
    }

    ///////////////////////////////////////////////////////////////

    public long count() throws Exception {
        return super.count(INDEX);
    }

    public long count(String datasource) throws Exception {
        return super.count(INDEX, datasource);
    }

    ///////////////////////////////////////////////////////////////

    public String createDocument(ElasticVertex document) throws Exception {
        return super.createDocument(INDEX, ElasticVertex.class, document);
    }

    public String updateDocument(ElasticVertex document) throws Exception {
        return super.updateDocument(INDEX, ElasticVertex.class, document);
    }

    public String deleteDocument(String id) throws Exception {
        return super.deleteDocument(INDEX, id);
    }
    public long deleteDocuments(String datasource) throws Exception {
        return super.deleteDocuments(INDEX, datasource);
    }

    ///////////////////////////////////////////////////////////////

    public List<ElasticVertex> findAll() throws Exception {
        return super.findAll(INDEX, ElasticVertex.class);
    }

    public ElasticVertex findById(String id) throws Exception {
        return super.findById(INDEX, ElasticVertex.class, id);
    }

    public boolean existsId(String id) throws Exception {
        return super.existsId(INDEX, id);
    }

    ///////////////////////////////////////////////////////////////

    public List<ElasticVertex> findByIds(String[] ids) throws Exception {
        return super.findByIds(INDEX, ElasticVertex.class, ids);
    }

    public List<ElasticVertex> findByLabel(int size, String label) throws Exception {
        return super.findByLabel(INDEX, ElasticVertex.class, size, label);
    }

    public List<ElasticVertex> findByDatasource(int size, String datasource) throws Exception {
        return super.findByDatasource(INDEX, ElasticVertex.class, size, datasource);
    }

    public List<ElasticVertex> findByDatasourceAndLabels(int size, String datasource, String[] labels) throws Exception {
        return super.findByDatasourceAndLabels(INDEX, ElasticVertex.class, size, datasource, labels);
    }

    public List<ElasticVertex> findByDatasourceAndPropertyKeys(int size, String datasource, String[] keys) throws Exception{
        return super.findByDatasourceAndPropertyKeys(INDEX, ElasticVertex.class, size, datasource, keys);
    }

    public List<ElasticVertex> findByDatasourceAndPropertyKey(int size, String datasource, String key) throws Exception{
        return super.findByDatasourceAndPropertyKey(INDEX, ElasticVertex.class, size, datasource, key);
    }

    public List<ElasticVertex> findByDatasourceAndPropertyKeyNot(int size, String datasource, String key) throws Exception{
        return super.findByDatasourceAndPropertyKeyNot(INDEX, ElasticVertex.class, size, datasource, key);
    }

    public List<ElasticVertex> findByDatasourceAndPropertyValues(int size, String datasource, String[] values) throws Exception{
        return super.findByDatasourceAndPropertyValues(INDEX, ElasticVertex.class, size, datasource, values);
    }

    public List<ElasticVertex> findByDatasourceAndPropertyValuePartial(int size, String datasource, String value) throws Exception{
        return super.findByDatasourceAndPropertyValuePartial(INDEX, ElasticVertex.class, size, datasource, value);
    }

    public List<ElasticVertex> findByDatasourceAndPropertyKeyValue(int size, String datasource, String key, String value) throws Exception{
        return super.findByDatasourceAndPropertyKeyValue(INDEX, ElasticVertex.class, size, datasource, key, value);
    }

    public List<ElasticVertex> findByDatasourceAndLabelAndPropertyKeyValue(int size, String datasource, String label, String key, String value) throws Exception{
        return super.findByDatasourceAndLabelAndPropertyKeyValue(INDEX, ElasticVertex.class, size, datasource, label, key, value);
    }

    public List<ElasticVertex> findByHasContainers(int size, String datasource
            , String label, String[] labels
            , String key, String keyNot, String[] keys
            , String[] values, Map<String,String> kvPairs) throws Exception {
        return super.findByHasContainers(INDEX, ElasticVertex.class, size, datasource
                , label, labels, key, keyNot, keys, values, kvPairs);
    }

}
