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
import org.springframework.util.ResourceUtils;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

import static com.example.aws.elasticsearch.demo.util.Constant.INDEX;
// import static com.example.aws.elasticsearch.demo.util.Constant.TYPE;
import static org.elasticsearch.index.query.QueryBuilders.*;

@Service
@Slf4j
public class ProfileService {

    static String mappings_file = "classpath:mappings/profile-document.json";

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

    // 참고 : mappings - types
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html
    // simple type : text, keyword, date, long, double, boolean, ip
    // JSON type : object, nested

    @PostConstruct
    private void ready() throws Exception {
        // if not exists index, create index
        if( !checkExistsIndex() ) createIndex();
    }

    // ** check url:
    //      mappings => http://27.117.163.21:15619/lead/_mappings
    //      first doc => http://27.117.163.21:15619/lead/_doc/profile_01

    ///////////////////////////////////////////////////////////////

    // check if exists index
    public boolean checkExistsIndex() throws  Exception {
        GetIndexRequest request = new GetIndexRequest(INDEX);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    private String readMappings() throws Exception {
        File file = ResourceUtils.getFile(mappings_file);
        if( !file.exists() ) throw new FileNotFoundException("mappings-file not found => "+mappings_file);
        return new String(Files.readAllBytes(file.toPath()));
    }

    public boolean createIndex() throws Exception {

        CreateIndexRequest request = new CreateIndexRequest(INDEX);

        // settings
        request.settings(Settings.builder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 0)
        );
        // mappings
        request.mapping(readMappings(), XContentType.JSON);

        AcknowledgedResponse indexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        return indexResponse.isAcknowledged();
    }

    public boolean removeIndex() throws Exception {
        DeleteIndexRequest request = new DeleteIndexRequest(INDEX);
        AcknowledgedResponse indexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
        return indexResponse.isAcknowledged();
    }

    public boolean resetIndex() throws Exception {
        // check if exists index
        if( checkExistsIndex() ) removeIndex();
        return createIndex();
    }

    ///////////////////////////////////////////////////////////////
    
    public String createProfileDocument(ProfileDocument document) throws Exception {

        // random document_id
        if( document.getId() == null ){
            UUID uuid = UUID.randomUUID();
            document.setId(uuid.toString());
        }

        IndexRequest indexRequest = new IndexRequest(INDEX)
                .id(document.getId())
                .source(convertProfileDocumentToMap(document));

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        return indexResponse.getResult().name();
    }

    // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs-update.html
    public String updateProfileDocument(ProfileDocument document) throws Exception {

        ProfileDocument resultDocument = findById(document.getId());

        UpdateRequest updateRequest = new UpdateRequest().index(INDEX)
                .id(resultDocument.getId())
                .doc(convertProfileDocumentToMap(document));

        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        return updateResponse.getResult().name();
    }

    public String deleteProfileDocument(String id) throws Exception {

        DeleteRequest deleteRequest = new DeleteRequest(INDEX)
                .id(id);

        DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
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

        GetRequest getRequest = new GetRequest(INDEX)
                .id(id);

        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        Map<String, Object> resultMap = getResponse.getSource();
        return convertMapToProfileDocument(resultMap);
    }

    ///////////////////////////////////////////////////////////////

//    private Map<String, Object> skipIdField(Map<String, Object> inputMap) {
//        return inputMap.entrySet().stream()
//                .filter(e->!e.getKey().equals("id"))
//                .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
//    }

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

        // define : nested query
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
            .must(QueryBuilders.nestedQuery("technologies",
                    QueryBuilders.boolQuery().must(
                        QueryBuilders.termQuery("technologies.name", technology)
                    )
            , ScoreMode.Avg) );

        // build
        searchSourceBuilder.query(queryBuilder);
        // set to request
        searchRequest.source(searchSourceBuilder);
        // search
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        // response
        return getSearchResult(response);
    }

    public List<ProfileDocument> searchByName(String name) throws Exception{

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

}
