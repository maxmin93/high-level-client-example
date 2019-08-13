package com.example.aws.elasticsearch.demo.service;

import com.example.aws.elasticsearch.demo.document.ProfileDocument;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

import static com.example.aws.elasticsearch.demo.util.Constant.INDEX;
import static com.example.aws.elasticsearch.demo.util.Constant.TYPE;
import static org.elasticsearch.index.query.QueryBuilders.*;

@Service
@Slf4j
public class ProfileService {

    private RestHighLevelClient client;
    private ObjectMapper objectMapper;

    @Autowired
    public ProfileService(
            RestHighLevelClient client,     // elasticsearch config
            ObjectMapper objectMapper       // spring boot web starter
    ) {
        this.client = client;
        this.objectMapper = objectMapper;
    }

    // 참고 : Java High Level REST Client
    // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high.html

    ///////////////////////////////////////////////////////////////

    // check if exists index
    public boolean checkExistsIndex() throws  Exception {
        GetIndexRequest request = new GetIndexRequest(INDEX);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    public boolean createIndex() throws Exception {

        // check if exists index
        if( checkExistsIndex() ) return true;

        CreateIndexRequest request = new CreateIndexRequest(INDEX);

        // settings
        request.settings(Settings.builder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", -1)
        );

        // mappings
        //
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html
        // simple type : text, keyword, date, long, double, boolean, ip
        // JSON type : object, nested
        request.mapping("{\n"
                +"  \"properties\": {\n"
                +"    \"first_name\"  : { \"type\": \"text\"    },\n"
                +"    \"last_name\"   : { \"type\": \"text\"    },\n"
                +"    \"gender\"      : { \"type\": \"keyword\" },\n"
                +"    \"age\"         : { \"type\": \"integer\" },\n"
                +"    \"emails\"      : { \"type\": \"keyword\" },\n"
                +"    \"technologies\": {\n"
                +"      \"type\": \"nested\",\n"
                +"      \"properties\": {\n"
                +"        \"name\"                : { \"type\": \"keyword\" },\n"
                +"        \"years_of_experience\" : { \"type\": \"integer\" }\n"
                +"      }\n"
                +"    }\n"
                +"  }\n"
                +"}\n", XContentType.JSON);

/*
{
  "properties":{
    "first_name" : { "type" : "text" },
    "last_name" : { "type" : "text" },
    "gender" : { "type" : "keyword" },
    "age" : { "type" : "integer" },
    "emails" : { "type" : "keyword" },
    "technologies" : {
      "type" : "nested",
      "properties": {
        "keyword": { "type": "keyword" },
        "years_of_experience": { "type": "integer" }
      }
    }
  }
}
 */
        AcknowledgedResponse indexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        return indexResponse.isAcknowledged();
    }

    public boolean removeIndex() throws Exception {
        // check if exists index
        if( !checkExistsIndex() ) return true;

        DeleteIndexRequest request = new DeleteIndexRequest(INDEX);
        AcknowledgedResponse indexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
        return indexResponse.isAcknowledged();
    }

    ///////////////////////////////////////////////////////////////
    
    public String createProfileDocument(ProfileDocument document) throws Exception {

        // check if exists index
        if( !checkExistsIndex() ) createIndex();

        // random document_id
        if( document.getId() == null ){
            UUID uuid = UUID.randomUUID();
            document.setId(uuid.toString());
        }

        IndexRequest indexRequest = new IndexRequest(INDEX)
                .source(convertProfileDocumentToMap(document));

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        return indexResponse.getResult().name();
    }

    public String updateProfileDocument(ProfileDocument document) throws Exception {

        ProfileDocument resultDocument = findById(document.getId());

        UpdateRequest updateRequest = new UpdateRequest(INDEX, resultDocument.getId());

        updateRequest.doc(convertProfileDocumentToMap(document));
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);

        return updateResponse.getResult().name();
    }

    public String deleteProfileDocument(String id) throws Exception {

        DeleteRequest deleteRequest = new DeleteRequest(INDEX);
        DeleteResponse response = client.delete(deleteRequest,RequestOptions.DEFAULT);

        return response.getResult().name();
    }

    ///////////////////////////////////////////////////////////////

    public List<ProfileDocument> findAll() throws Exception {

        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse =
                client.search(searchRequest, RequestOptions.DEFAULT);

        return getSearchResult(searchResponse);
    }

    public ProfileDocument findById(String id) throws Exception {

        GetRequest getRequest = new GetRequest(INDEX);  //, TYPE, id);

        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        Map<String, Object> resultMap = getResponse.getSource();

        return convertMapToProfileDocument(resultMap);
    }

    public List<ProfileDocument> findProfileByName(String name) throws Exception{

        SearchRequest searchRequest = new SearchRequest(INDEX);
//        searchRequest.indices(INDEX);
//        searchRequest.types(TYPE);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        MatchQueryBuilder matchQueryBuilder = QueryBuilders
                .matchQuery("firstName",name)
                .operator(Operator.AND);

        searchSourceBuilder.query(matchQueryBuilder);

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        return getSearchResult(searchResponse);
    }

    ///////////////////////////////////////////////////////////////

    private Map<String, Object> convertProfileDocumentToMap(ProfileDocument profileDocument) {
        return objectMapper.convertValue(profileDocument, Map.class);
    }

    private ProfileDocument convertMapToProfileDocument(Map<String, Object> map){
        return objectMapper.convertValue(map, ProfileDocument.class);
    }

    ///////////////////////////////////////////////////////////////

    public List<ProfileDocument> searchByTechnology(String technology) throws Exception{

        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        QueryBuilder queryBuilder = boolQuery()
            .must(nestedQuery("technologies", boolQuery()
                    .must(QueryBuilders.termQuery("technologies.name",technology))
                , ScoreMode.Avg)
            );

        // build
        searchSourceBuilder.query(nestedQuery("technologies",queryBuilder,ScoreMode.Avg));
        // set to request
        searchRequest.source(searchSourceBuilder);
        // search
        SearchResponse response = client.search(searchRequest,RequestOptions.DEFAULT);
        // response
        return getSearchResult(response);
    }

    public List<ProfileDocument> searchByName(String name) throws Exception{

        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // define
        BoolQueryBuilder filter = new BoolQueryBuilder()
                .should(termQuery("firstName",name))
                .should(termQuery("lastName",name));

        // build
        searchSourceBuilder.query(filter);
        // set to request
        searchRequest.source(searchSourceBuilder);
        // search
        SearchResponse response = client.search(searchRequest,RequestOptions.DEFAULT);
        // response
        return getSearchResult(response);
    }

    private List<ProfileDocument> getSearchResult(SearchResponse response) {

        SearchHit[] searchHit = response.getHits().getHits();

        List<ProfileDocument> profileDocuments = new ArrayList<>();

        for (SearchHit hit : searchHit){
            profileDocuments.add(objectMapper.convertValue(
                    hit.getSourceAsMap(), ProfileDocument.class));
        }

        return profileDocuments;
    }

//    private SearchRequest buildSearchRequest(String index, String type) {
//
//        SearchRequest searchRequest = new SearchRequest();
//        searchRequest.indices(index);
//        searchRequest.types(type);
//
//        return searchRequest;
//    }
}
