package com.example.aws.elasticsearch.demo.elasticgraph.util;

import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticEdgeDocument;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticVertexDocument;
import com.example.aws.elasticsearch.demo.profilesample.model.ProfileDocument;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public final class ElasticGraphHelper {

    public static Map<String, Object> convertVertexDocumentToMap(ObjectMapper mapper, ElasticVertexDocument vertexDocument) {
        return mapper.convertValue(vertexDocument, Map.class);
    }
    public static Map<String, Object> convertEdgeDocumentToMap(ObjectMapper mapper, ElasticEdgeDocument edgeDocument) {
        return mapper.convertValue(edgeDocument, Map.class);
    }

    public static ElasticEdgeDocument convertMapToEdgeDocument(ObjectMapper mapper, Map<String, Object> map){
        return mapper.convertValue(map, ElasticEdgeDocument.class);
    }
    public static ElasticVertexDocument convertMapToVertexDocument(ObjectMapper mapper, Map<String, Object> map){
        return mapper.convertValue(map, ElasticVertexDocument.class);
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
