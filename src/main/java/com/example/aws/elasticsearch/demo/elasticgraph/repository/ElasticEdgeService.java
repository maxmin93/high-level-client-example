package com.example.aws.elasticsearch.demo.elasticgraph.repository;

import com.example.aws.elasticsearch.demo.basegraph.BaseGraphAPI;
import com.example.aws.elasticsearch.demo.basegraph.model.BaseProperty;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticEdge;

import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticProperty;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticVertex;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

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

    public boolean existsId(String id) throws Exception {
        return super.existsId(INDEX, id);
    }

    ///////////////////////////////////////////////////////////////

    public List<ElasticEdge> findByIds(String[] ids) throws Exception {
        return super.findByIds(INDEX, ElasticEdge.class, ids);
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

    public List<ElasticEdge> findByDatasourceAndPropertyKeys(int size, String datasource, String[] keys) throws Exception{
        return super.findByDatasourceAndPropertyKeys(INDEX, ElasticEdge.class, size, datasource, keys);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyKey(int size, String datasource, String key) throws Exception{
        return super.findByDatasourceAndPropertyKey(INDEX, ElasticEdge.class, size, datasource, key);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyKeyNot(int size, String datasource, String key) throws Exception{
        return super.findByDatasourceAndPropertyKeyNot(INDEX, ElasticEdge.class, size, datasource, key);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyValues(int size, String datasource, String[] values) throws Exception{
        List<ElasticEdge> list = super.findByDatasourceAndPropertyValues(INDEX, ElasticEdge.class, size, datasource, values);
        // compare two values by full match with lowercase
        List<String> fvalues = Arrays.asList(values).stream().map(String::toLowerCase).collect(Collectors.toList());
        List<ElasticEdge> filteredList = new ArrayList<>();
        for( ElasticEdge edge : list ){
            List<String> pvalues = edge.getProperties().stream().map(p->((ElasticProperty)p).getValue().toLowerCase()).collect(Collectors.toList());
            if( pvalues.containsAll(fvalues) ) filteredList.add(edge);
        }
        return filteredList;
    }

    public List<ElasticEdge> findByDatasourceAndPropertyValuePartial(int size, String datasource, String value) throws Exception{
        return super.findByDatasourceAndPropertyValuePartial(INDEX, ElasticEdge.class, size, datasource, value);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyKeyValue(int size, String datasource, String key, String value) throws Exception{
        List<ElasticEdge> list = super.findByDatasourceAndPropertyKeyValue(INDEX, ElasticEdge.class, size, datasource, key, value);
        return list.stream().filter(r-> {
                    for(BaseProperty p : r.getProperties()){
                        if( ((ElasticProperty)p).getKey().equals(key) ){
                            if( ((ElasticProperty)p).getValue().equalsIgnoreCase(value) ) return true;
                        }
                    }
                    return false;
                }).collect(Collectors.toList());
    }

    public List<ElasticEdge> findByDatasourceAndLabelAndPropertyKeyValue(int size, String datasource, String label, String key, String value) throws Exception{
        List<ElasticEdge> list = super.findByDatasourceAndLabelAndPropertyKeyValue(INDEX, ElasticEdge.class, size, datasource, label, key, value);
        return list.stream().filter(r-> {
                    for(BaseProperty p : r.getProperties()){
                        if( ((ElasticProperty)p).getKey().equals(key) ){
                            if( ((ElasticProperty)p).getValue().equalsIgnoreCase(value) ) return true;
                        }
                    }
                    return false;
                }).collect(Collectors.toList());
    }

    ///////////////////////////////////////////////////////////////

    public List<ElasticEdge> findByDatasourceAndDirection(
            int size, String datasource, String vid, BaseGraphAPI.Direction direction) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));
        // with direction
        if( direction.equals(BaseGraphAPI.Direction.IN))
            queryBuilder = queryBuilder.must(termQuery("tid", vid));
        else if( direction.equals(BaseGraphAPI.Direction.OUT))
            queryBuilder = queryBuilder.must(termQuery("sid", vid));
        else{
            queryBuilder = queryBuilder.should(termQuery("tid", vid));
            queryBuilder = queryBuilder.should(termQuery("sid", vid));
        }
        // search
        return doSearch(INDEX, size, queryBuilder, client, mapper, ElasticEdge.class);
    }
}
