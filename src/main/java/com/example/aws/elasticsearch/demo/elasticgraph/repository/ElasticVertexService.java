package com.example.aws.elasticsearch.demo.elasticgraph.repository;

import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticVertexDocument;
import com.example.aws.elasticsearch.demo.elasticgraph.util.ElasticGraphHelper;
import com.example.aws.elasticsearch.demo.profilesample.model.ProfileDocument;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.example.aws.elasticsearch.demo.profilesample.Constant.INDEX;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@Service
@Slf4j
public class ElasticVertexService {

    private RestHighLevelClient client;
    private ObjectMapper mapper;
    private final String INDEX;

    @Autowired
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
        // random document_id
        if( document.getId() == null ){
            UUID uuid = UUID.randomUUID();
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

    public List<ElasticVertexDocument> searchByLabel(String label) throws Exception {

        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // define : or match
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(termQuery("label", label));

        // build
        searchSourceBuilder.query(queryBuilder);
        // set to request
        searchRequest.source(searchSourceBuilder);
        // search
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // response
        return ElasticGraphHelper.getSearchResult(response, mapper, ElasticVertexDocument.class);
    }

    public List<ElasticVertexDocument> searchByProperyKey(String key) throws Exception{
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // define : nested query
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.nestedQuery("properties",
                        QueryBuilders.boolQuery().must(
                                QueryBuilders.termQuery("properties.key", key)
                        )
                        , ScoreMode.Avg) );

        // build
        searchSourceBuilder.query(queryBuilder);
        // set to request
        searchRequest.source(searchSourceBuilder);
        // search
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // response
        return ElasticGraphHelper.getSearchResult(response, mapper, ElasticVertexDocument.class);
    }

    public List<ElasticVertexDocument> searchByName(String name) throws Exception {

        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // define : or match
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .should(termQuery("first_name", name))
                .should(termQuery("last_name", name));

        // build
        searchSourceBuilder.query(queryBuilder);
        // set to request
        searchRequest.source(searchSourceBuilder);
        // search
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // response
        return ElasticGraphHelper.getSearchResult(response, mapper, ElasticVertexDocument.class);
    }
}
