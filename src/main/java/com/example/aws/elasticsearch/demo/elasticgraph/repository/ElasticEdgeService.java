package com.example.aws.elasticsearch.demo.elasticgraph.repository;

import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticEdge;

import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticProperty;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticVertex;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public final class ElasticEdgeService extends ElasticElementService {

    private final String INDEX;

    public ElasticEdgeService(
            RestHighLevelClient client,     // elasticsearch config
            ObjectMapper mapper             // spring boot web starter
    ) {
        super(client, mapper);
        this.INDEX = ElasticGraphService.INDEX_EDGE;
    }

    ///////////////////////////////////////////////////////////////

    public long count() throws Exception {
        return super.count(INDEX);
    }

    public long count(String datasource) throws Exception {
        return super.count(INDEX, datasource);
    }

    ///////////////////////////////////////////////////////////////

    public String createDocument(ElasticEdge document) throws Exception {
        return super.createDocument(INDEX, ElasticEdge.class, document);
    }

    public String updateDocument(ElasticEdge document) throws Exception {
        return super.updateDocument(INDEX, ElasticEdge.class, document);
    }

    public String deleteDocument(String id) throws Exception {
        return super.deleteDocument(INDEX, id);
    }
    public long deleteDocuments(String datasource) throws Exception {
        return super.deleteDocuments(INDEX, datasource);
    }

    ///////////////////////////////////////////////////////////////

    public List<ElasticEdge> findAll() throws Exception {
        return super.findAll(INDEX, ElasticEdge.class);
    }

    public ElasticEdge findById(String id) throws Exception {
        return super.findById(INDEX, ElasticEdge.class, id);
    }

    ///////////////////////////////////////////////////////////////

    public List<ElasticEdge> findByIDs(String[] ids) throws Exception {
        return super.findByIDs(INDEX, ElasticEdge.class, ids);
    }

    public List<ElasticEdge> findByLabel(int size, String label) throws Exception {
        return super.findByLabel(INDEX, ElasticEdge.class, size, label);
    }

    public List<ElasticEdge> findByDatasource(int size, String datasource) throws Exception {
        return super.findByDatasource(INDEX, ElasticEdge.class, size, datasource);
    }

    public List<ElasticEdge> findByDatasourceAndLabels(int size, String datasource, String[] labels) throws Exception {
        return super.findByDatasourceAndLabels(INDEX, ElasticEdge.class, size, datasource, labels);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyKey(int size, String datasource, String key) throws Exception{
        return super.findByDatasourceAndPropertyKey(INDEX, ElasticEdge.class, size, datasource, key);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyValue(int size, String datasource, String value) throws Exception{
        List<ElasticEdge> list = super.findByDatasourceAndPropertyValue(INDEX, ElasticEdge.class, size, datasource, value);
        return list.stream().filter(r-> {
                    for(ElasticProperty p : r.getProperties()){
                        if( p.getValue().equalsIgnoreCase(value) ) return true;
                    }
                    return false;
                }).collect(Collectors.toList());
    }
    public List<ElasticEdge> findByDatasourceAndPropertyValuePartial(int size, String datasource, String value) throws Exception{
        return super.findByDatasourceAndPropertyValuePartial(INDEX, ElasticEdge.class, size, datasource, value);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyKeyValue(int size, String datasource, String key, String value) throws Exception{
        List<ElasticEdge> list = super.findByDatasourceAndPropertyKeyValue(INDEX, ElasticEdge.class, size, datasource, key, value);
        return list.stream().filter(r-> {
                    for(ElasticProperty p : r.getProperties()){
                        if( p.getKey().equals(key) ){
                            if( p.getValue().equalsIgnoreCase(value) ) return true;
                        }
                    }
                    return false;
                }).collect(Collectors.toList());
    }

    public List<ElasticEdge> findByDatasourceAndLabelAndPropertyKeyValue(int size, String datasource, String label, String key, String value) throws Exception{
        List<ElasticEdge> list = super.findByDatasourceAndLabelAndPropertyKeyValue(INDEX, ElasticEdge.class, size, datasource, label, key, value);
        return list.stream().filter(r-> {
                    for(ElasticProperty p : r.getProperties()){
                        if( p.getKey().equals(key) ){
                            if( p.getValue().equalsIgnoreCase(value) ) return true;
                        }
                    }
                    return false;
                }).collect(Collectors.toList());
    }

}
