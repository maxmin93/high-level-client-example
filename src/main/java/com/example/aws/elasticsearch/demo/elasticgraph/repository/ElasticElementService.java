package com.example.aws.elasticsearch.demo.elasticgraph.repository;

import com.example.aws.elasticsearch.demo.basegraph.model.BaseProperty;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticEdge;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticElement;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticProperty;
import com.example.aws.elasticsearch.demo.elasticgraph.util.ElasticHelper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

public class ElasticElementService {

    protected final RestHighLevelClient client;
    protected final ObjectMapper mapper;

    protected ElasticElementService(
            RestHighLevelClient client,     // elasticsearch config
            ObjectMapper mapper             // spring boot web starter
    ) {
        this.client = client;
        this.mapper = mapper;
    }

    ///////////////////////////////////////////////////////////////

    protected <T> String createDocument(String index, Class<T> tClass, ElasticElement document) throws Exception {
        if( document.getId() == null || document.getId().isEmpty() ){
            UUID uuid = UUID.randomUUID();      // random document_id
            document.setId(uuid.toString());
        }

        IndexRequest indexRequest = new IndexRequest(index)
                .id(document.getId())
                .source( mapper.convertValue((T)document, Map.class) );
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        return indexResponse.getResult().name();
    }

    protected <T> String updateDocument(String index, Class<T> tClass, ElasticElement document) throws Exception {
        if( document.getId() == null || document.getId().isEmpty() )
            return "NOT_FOUND";

        UpdateRequest updateRequest = new UpdateRequest().index(index)
                .id(document.getId())
                .doc( mapper.convertValue((T)document, Map.class) );
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        return updateResponse.getResult().name();
    }

    protected String deleteDocument(String index, String id) throws Exception {
        DeleteRequest deleteRequest = new DeleteRequest(index).id(id);
        DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
        return response.getResult().name();
    }

    protected long deleteDocuments(String index, String datasource) throws Exception {
        DeleteByQueryRequest request = new DeleteByQueryRequest(index);
        request.setQuery(new TermQueryBuilder("datasource", datasource));
        // request.setSlices(2);
        request.setRefresh(true);
        request.setConflicts("proceed");
        BulkByScrollResponse bulkResponse = client.deleteByQuery(request, RequestOptions.DEFAULT);
        return bulkResponse.getDeleted();
    }

    ///////////////////////////////////////////////////////////////

    protected long count(String index) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        CountRequest countRequest = new CountRequest().indices(index);
        countRequest.source(searchSourceBuilder);
        CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
        return countResponse.getCount();
    }

    protected long count(String index, String datasource) throws Exception {
        // match to datasource
        BoolQueryBuilder queryBuilder = ElasticHelper.addQueryDs(QueryBuilders.boolQuery(), datasource);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);

        CountRequest countRequest = new CountRequest().indices(index);
        countRequest.source(searchSourceBuilder);
        CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
        return countResponse.getCount();
    }

    ///////////////////////////////////////////////////////////////

    protected <T> List<T> findAll(String index, Class<T> tClass) throws Exception {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());   // All
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return getSearchResult(searchResponse, mapper, tClass);
    }

    protected <T> T findById(String index, Class<T> tClass, String id) throws Exception {
        GetRequest getRequest = new GetRequest(index).id(id);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        Map<String, Object> resultMap = getResponse.getSource();
        return mapper.convertValue(resultMap, tClass);
    }

    protected boolean existsId(String index, String id) throws Exception {
        GetRequest getRequest = new GetRequest(index, id);
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        return client.exists(getRequest, RequestOptions.DEFAULT);
    }

    ///////////////////////////////////////////////////////////////

    // DS.hadId(id..)
    protected <T> List<T> findByIds(String index, Class<T> tClass, String[] ids) throws Exception {
        // match to datasource
        IdsQueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds(ids);
        // search
        return doSearch(index, ids.length, queryBuilder, client, mapper, tClass);
    }

    protected <T> List<T> findByLabel(String index, Class<T> tClass, int size, String label) throws Exception {
        // match to label
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("label", label));
        // search
        return doSearch(index, size, queryBuilder, client, mapper, tClass);
    }

    // DS.V(), DS.E()
    protected <T> List<T> findByDatasource(String index, Class<T> tClass, int size, String datasource) throws Exception {
        // match to datasource
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));
        // search
        return doSearch(index, size, queryBuilder, client, mapper, tClass);
    }

    // DS.hasLabel(label..)
    protected <T> List<T> findByDatasourceAndLabels(String index, Class<T> tClass, int size, String datasource, String[] labels) throws Exception {
        // match to datasource
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .filter(termsQuery("label", labels));
        // search
        return doSearch(index, size, queryBuilder, client, mapper, tClass);
    }

    // DS.hasKey(key..)
    protected <T> List<T> findByDatasourceAndPropertyKeys(
            String index, Class<T> tClass, int size, String datasource, String[] keys) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));
        // AND
        for( String key : keys ){
            queryBuilder = queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                        termQuery("properties.key", key)
                    ), ScoreMode.Max));
        }
        // search
        return doSearch(index, size, queryBuilder, client, mapper, tClass);
    }

    // DS.has(key)
    protected <T> List<T> findByDatasourceAndPropertyKey(
            String index, Class<T> tClass, int size, String datasource, String key) throws Exception{
        // define : nested query
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .must(QueryBuilders.nestedQuery("properties",
                        QueryBuilders.boolQuery().must(
                            termQuery("properties.key", key)
                        ), ScoreMode.Avg));
        // search
        return doSearch(index, size, queryBuilder, client, mapper, tClass);
    }

    // DS.hasNot(key)
    protected <T> List<T> findByDatasourceAndPropertyKeyNot(
            String index, Class<T> tClass, int size, String datasource, String key) throws Exception{
        // define : nested query
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .mustNot(QueryBuilders.nestedQuery("properties",
                        QueryBuilders.boolQuery().must(
                            termQuery("properties.key", key)
                        ), ScoreMode.Avg));
        // search
        return doSearch(index, size, queryBuilder, client, mapper, tClass);
    }

    // DS.hasValue(value..)
    protected <T> List<T> findByDatasourceAndPropertyValues(
            String index, Class<T> tClass, int size, String datasource, String[] values) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));
        // AND
        for( String value : values ) {
            queryBuilder = queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                        QueryBuilders.queryStringQuery("properties.value:\"" + value.toLowerCase() + "\"")
                    ), ScoreMode.Total));
        }
        // search
        List<T> list = doSearch(index, size, queryBuilder, client, mapper, tClass);
        // compare two values by full match with lowercase
        List<String> fvalues = Arrays.asList(values).stream().map(String::toLowerCase).collect(Collectors.toList());
        List<T> filteredList = new ArrayList<>();
        for( T item : list ){
            List<String> pvalues = ((ElasticElement)item).getProperties().stream().map(p->p.getValue().toLowerCase()).collect(Collectors.toList());
            if( pvalues.containsAll(fvalues) ) filteredList.add(item);
        }
        return filteredList;
    }

    // DS.hasValuePartial(value)
    protected <T> List<T> findByDatasourceAndPropertyValuePartial(
            String index, Class<T> tClass, int size, String datasource, String value) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .must(QueryBuilders.nestedQuery("properties",
                        QueryBuilders.boolQuery().must(
                            // QueryBuilders.wildcardQuery("properties.value", "*"+value.toLowerCase()+"*")
                            QueryBuilders.queryStringQuery("properties.value:*"+value.toLowerCase()+"*")
                        ), ScoreMode.Avg));
        // search
        return doSearch(index, size, queryBuilder, client, mapper, tClass);
    }

    // DS.has(key,value)
    protected <T> List<T> findByDatasourceAndPropertyKeyValue(
            String index, Class<T> tClass, int size, String datasource, String key, String value) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .must(QueryBuilders.nestedQuery("properties", QueryBuilders.boolQuery()
                            .must(termQuery("properties.key", key.toLowerCase()))
                            .must(QueryBuilders.queryStringQuery("properties.value:\""+value.toLowerCase()+"\""))
                    , ScoreMode.Total));
                ;
        // search
        List<T> list = doSearch(index, size, queryBuilder, client, mapper, tClass);
        return list.stream().filter(r-> {
            for(ElasticProperty p : ((ElasticElement)r).getProperties()){
                if( p.getKey().equals(key) ){
                    if( p.getValue().equalsIgnoreCase(value) ) return true;
                }
            }
            return false;
        }).collect(Collectors.toList());
    }

    // DS.has(label,key,value)
    protected <T> List<T> findByDatasourceAndLabelAndPropertyKeyValue(
            String index, Class<T> tClass, int size, String datasource, String label, String key, String value) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .filter(termQuery("label", label))
                .must(QueryBuilders.nestedQuery("properties", QueryBuilders.boolQuery()
                            .must(termQuery("properties.key", key.toLowerCase()))
                            .must(QueryBuilders.queryStringQuery("properties.value:\""+value.toLowerCase()+"\""))
                    , ScoreMode.Total));
        // search
        List<T> list = doSearch(index, size, queryBuilder, client, mapper, tClass);
        return list.stream().filter(r-> {
            for(ElasticProperty p : ((ElasticElement)r).getProperties()){
                if( p.getKey().equals(key) ){
                    if( p.getValue().equalsIgnoreCase(value) ) return true;
                }
            }
            return false;
        }).collect(Collectors.toList());
    }

    ///////////////////////////////////////////////////////////////

    protected <T> List<T> findByHasContainers(
            String index, Class<T> tClass, int size, String datasource
            , String label, String[] labels
            , String key, String keyNot, String[] keys
            , String[] values, Map<String,String> keyValues) throws Exception {

        // init
        BoolQueryBuilder qb = ElasticHelper.addQueryDs(QueryBuilders.boolQuery(), datasource);
        // AND hasCondition
        if( label != null ) qb = ElasticHelper.addQueryLabel(qb, label);
        if( labels != null && labels.length > 0 ) qb = ElasticHelper.addQueryLabels(qb, labels);
        if( key != null ) qb = ElasticHelper.addQueryKey(qb, key);
        if( keyNot != null ) qb = ElasticHelper.addQueryKeyNot(qb, keyNot);
        if( keys != null && keys.length > 0 ) qb = ElasticHelper.addQueryKeys(qb, keys);
        if( values != null && values.length > 0 ) qb = ElasticHelper.addQueryValues(qb, values);
        if( keyValues != null && keyValues.size() > 0 ){
            for(Map.Entry<String,String> kv : keyValues.entrySet()){
                qb = ElasticHelper.addQueryKeyValue(qb, kv.getKey(), kv.getValue());
            }
        }

        // search
        List<T> list = doSearch(index, size, qb, client, mapper, tClass);

        // post filters
        if( values != null && values.length > 0 ){
            // compare two values by full match with lowercase
            List<String> fvalues = Arrays.asList(values).stream().map(String::toLowerCase).collect(Collectors.toList());
            List<T> temp = new ArrayList<>();
            for( T item : list ){
                List<String> pvalues = ((ElasticElement)item).getProperties().stream().map(p->p.getValue().toLowerCase()).collect(Collectors.toList());
                if( pvalues.containsAll(fvalues) ) temp.add(item);
            }
            list = temp;
        }
        if( keyValues != null && keyValues.size() > 0 ){
            List<T> temp = list;
            for(Map.Entry<String,String> kv : keyValues.entrySet()){
                temp = temp.stream().filter(r-> {
                    for(ElasticProperty p : ((ElasticElement)r).getProperties()){
                        if( p.getKey().equals(kv.getKey()) ){
                            if( p.getValue().equalsIgnoreCase(kv.getValue()) ) return true;
                        }
                    }
                    return false;
                }).collect(Collectors.toList());
            }
            list = temp;
        }
        return list;
    }

    protected final <T> List<T> getSearchResult(SearchResponse response, ObjectMapper mapper, Class<T> tClass) {
        SearchHit[] searchHit = response.getHits().getHits();
        List<T> documents = new ArrayList<>();
        for (SearchHit hit : searchHit){
            documents.add(mapper.convertValue(hit.getSourceAsMap(), tClass));
        }
        return documents;
    }

    protected final <T> List<T> doSearch(String index, int size, QueryBuilder queryBuilder
            , RestHighLevelClient client, ObjectMapper mapper, Class<T> tClass) throws Exception {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.size(size);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        return getSearchResult(response, mapper, tClass);
    }

}
