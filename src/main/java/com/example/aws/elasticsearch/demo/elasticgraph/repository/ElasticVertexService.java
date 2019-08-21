package com.example.aws.elasticsearch.demo.elasticgraph.repository;

import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticVertexDocument;
import com.example.aws.elasticsearch.demo.elasticgraph.util.ElasticGraphHelper;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@Slf4j
public class ElasticVertexService {

    private RestHighLevelClient client;
    private ObjectMapper mapper;
    private final String INDEX;

    public ElasticVertexService(
            RestHighLevelClient client,     // elasticsearch config
            ObjectMapper mapper             // spring boot web starter
    ) {
        this.client = client;
        this.mapper = mapper;
        this.INDEX = ElasticGraphService.INDEX_VERTEX;
    }

    ///////////////////////////////////////////////////////////////

    public String createDocument(ElasticVertexDocument document) throws Exception {
        if( document.getId() == null || document.getId().isEmpty() ){
            UUID uuid = UUID.randomUUID();      // random document_id
            document.setId(uuid.toString());
        }

        IndexRequest indexRequest = new IndexRequest(INDEX)
                .id(document.getId())
                .source(ElasticGraphHelper.convertVertexDocumentToMap(mapper, document));

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        return indexResponse.getResult().name();
    }

    public String updateDocument(ElasticVertexDocument document) throws Exception {
        ElasticVertexDocument existing = findById(document.getId());
        if( existing == null ) return createDocument(document);

        UpdateRequest updateRequest = new UpdateRequest().index(INDEX)
                .id(existing.getId())
                .doc(ElasticGraphHelper.convertVertexDocumentToMap(mapper, document));

        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        return updateResponse.getResult().name();
    }

    public String deleteDocument(String id) throws Exception {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX).id(id);
        DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
        return response.getResult().name();
    }

    ///////////////////////////////////////////////////////////////

    public List<ElasticVertexDocument> findAll() throws Exception {
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return ElasticGraphHelper.getSearchResult(searchResponse, mapper, ElasticVertexDocument.class);
    }

    public ElasticVertexDocument findById(String id) throws Exception {
        GetRequest getRequest = new GetRequest(INDEX).id(id);

        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        Map<String, Object> resultMap = getResponse.getSource();
        return ElasticGraphHelper.convertMapToVertexDocument(mapper, resultMap);
    }

    ///////////////////////////////////////////////////////////////

    public List<ElasticVertexDocument> findByLabel(int size, String label) throws Exception {
        // match to label
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("label", label));
        // search
        return ElasticGraphHelper.doSearch(INDEX, size, queryBuilder, client, mapper, ElasticVertexDocument.class);
    }

    public List<ElasticVertexDocument> findByDatasource(int size, String datasource) throws Exception {
        // match to datasource
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));
        // search
        return ElasticGraphHelper.doSearch(INDEX, size, queryBuilder, client, mapper, ElasticVertexDocument.class);
    }

    public List<ElasticVertexDocument> findByDatasourceAndLabel(int size, String datasource, String label) throws Exception {
        // match to datasource
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .filter(termQuery("label", label));
        // search
        return ElasticGraphHelper.doSearch(INDEX, size, queryBuilder, client, mapper, ElasticVertexDocument.class);
    }

    public List<ElasticVertexDocument> findByDatasourceAndPropertyKey(
                int size, String datasource, String key) throws Exception{
        // define : nested query
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .must(QueryBuilders.nestedQuery("properties",
                        QueryBuilders.boolQuery().must(
                                termQuery("properties.key", key)
                        )
                        , ScoreMode.None) );
        // search
        return ElasticGraphHelper.doSearch(INDEX, size, queryBuilder, client, mapper, ElasticVertexDocument.class);
    }

    public List<ElasticVertexDocument> findByDatasourceAndPropertyValue(
                int size, String datasource, String value) throws Exception{
        // define : nested query
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource))
                .must(QueryBuilders.nestedQuery("properties",
                        QueryBuilders.boolQuery().must(
                                termQuery("properties.value", value)
                        )
                        , ScoreMode.Total) );
        // search
        return ElasticGraphHelper.doSearch(INDEX, size, queryBuilder, client, mapper, ElasticVertexDocument.class);
    }
}
