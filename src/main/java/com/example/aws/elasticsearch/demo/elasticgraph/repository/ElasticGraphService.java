package com.example.aws.elasticsearch.demo.elasticgraph.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@Slf4j
public class ElasticGraphService {

    static final String MAPPINGS_VERTEX = "classpath:mappings/vertex-document.json";
    static final String MAPPINGS_EDGE = "classpath:mappings/edge-document.json";

    public static final String INDEX_VERTEX = "elasticvertex";
    public static final String INDEX_EDGE = "elasticedge";

    private RestHighLevelClient client;
    private ObjectMapper objectMapper;

    public ElasticGraphService(
            RestHighLevelClient client,     // elasticsearch config
            ObjectMapper objectMapper       // spring boot web starter
    ) {
        this.client = client;
        this.objectMapper = objectMapper;
    }

    ///////////////////////////////////////////////////////////////

    // check if exists index
    private boolean checkExistsIndex(String index) throws  Exception {
        GetIndexRequest request = new GetIndexRequest(index);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    private String readMappings(String index) throws Exception {
        String mappings_file = index.equals(INDEX_VERTEX) ? MAPPINGS_VERTEX : MAPPINGS_EDGE;
        File file = ResourceUtils.getFile(mappings_file);
        if( !file.exists() ) throw new FileNotFoundException("mappings not found => "+mappings_file);
        return new String(Files.readAllBytes(file.toPath()));
    }

    private boolean createIndex(String index) throws Exception {

        CreateIndexRequest request = new CreateIndexRequest(index);

        // settings
        request.settings(Settings.builder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 0)
        );
        // mappings
        request.mapping(readMappings(index), XContentType.JSON);

        AcknowledgedResponse indexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        return indexResponse.isAcknowledged();
    }

    private boolean removeIndex(String index) throws Exception {
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        AcknowledgedResponse indexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
        return indexResponse.isAcknowledged();
    }

    public boolean resetIndex() throws Exception {
        boolean result = true;

        if( checkExistsIndex(INDEX_VERTEX) ) removeIndex(INDEX_VERTEX);
        result &= createIndex(INDEX_VERTEX);
        if( checkExistsIndex(INDEX_EDGE) ) removeIndex(INDEX_EDGE);
        result &= createIndex(INDEX_EDGE);

        return result;
    }

    public void ready() throws Exception {
        // if not exists index, create index
        if( !checkExistsIndex(INDEX_VERTEX) ) createIndex(INDEX_VERTEX);
        if( !checkExistsIndex(INDEX_EDGE) ) createIndex(INDEX_EDGE);
    }

    //////////////////////////////////////////////
    // schema services

    // REST API : Aggregation
    // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/_metrics_aggregations.html

    public Map<String, Long> listLabels(String index, String datasource) throws Exception {
        // query : aggregation
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource)))
                .aggregation(AggregationBuilders.terms("labels").field("label").order(BucketOrder.key(true)));

        // request
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // response
        Aggregations aggregations = searchResponse.getAggregations();
        Terms labels = aggregations.get("labels");

        Map<String, Long> result = new HashMap<>();
        labels.getBuckets().forEach(b->{
            result.put(b.getKeyAsString(), b.getDocCount());
        });
        return result;
    }

    public Map<String, Long> listLabelKeys(String index, String datasource, String label) throws Exception {
        // query : aggregation
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery()
                    .filter(termQuery("datasource", datasource))
                    .filter(termQuery("label", label))
                )
                .aggregation(AggregationBuilders.nested("agg", "properties")
                    .subAggregation(
                        AggregationBuilders.terms("keys").field("properties.key")
                            .subAggregation(
                                AggregationBuilders.reverseNested("label_to_key")
                            )
                    ));

        // request
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // response
        Aggregations aggregations = searchResponse.getAggregations();
        Nested agg = aggregations.get("agg");
        Terms keys = agg.getAggregations().get("keys");

        Map<String, Long> result = new HashMap<>();
        keys.getBuckets().forEach(b->{
            result.put(b.getKeyAsString(), b.getDocCount());
        });
        return result;
    }

}
