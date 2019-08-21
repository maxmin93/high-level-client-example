package com.example.aws.elasticsearch.demo.elasticgraph.util;

import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticEdge;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticProperty;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticVertex;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public final class ElasticGraphHelper {

    public static Map<String, Object> convertVertexDocumentToMap(ObjectMapper mapper, ElasticVertex vertexDocument) {
        return mapper.convertValue(vertexDocument, Map.class);
    }
    public static Map<String, Object> convertEdgeDocumentToMap(ObjectMapper mapper, ElasticEdge edgeDocument) {
        return mapper.convertValue(edgeDocument, Map.class);
    }

    public static ElasticEdge convertMapToEdgeDocument(ObjectMapper mapper, Map<String, Object> map){
        return mapper.convertValue(map, ElasticEdge.class);
    }
    public static ElasticVertex convertMapToVertexDocument(ObjectMapper mapper, Map<String, Object> map){
        return mapper.convertValue(map, ElasticVertex.class);
    }

    public static Object value(ObjectMapper mapper, ElasticProperty property){
        if( property == null ) return null;
        Object translated = (Object)property.getValue();

        try {
            Class<?> clazz = Class.forName(property.getType());
            translated = (Object) mapper.convertValue(property.getValue(), clazz);
        }
        catch (ClassNotFoundException ex){
            return translated;
        }
        return translated;
    }

    public static <T> List<T> getSearchResult(SearchResponse response, ObjectMapper mapper, Class<T> tClass) {
        SearchHit[] searchHit = response.getHits().getHits();
        List<T> documents = new ArrayList<>();
        for (SearchHit hit : searchHit){
            documents.add(mapper.convertValue(hit.getSourceAsMap(), tClass));
        }
        return documents;
    }

    public static <T> List<T> doSearch(String index, int size, QueryBuilder queryBuilder
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
