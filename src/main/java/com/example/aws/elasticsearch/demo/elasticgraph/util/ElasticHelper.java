package com.example.aws.elasticsearch.demo.elasticgraph.util;

import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticEdge;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticElement;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticProperty;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticVertex;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

public final class ElasticHelper {

    // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-search.html
/*

    ** ScoreMode
    total : Add the original score and the rescore query score. The default.
    multiply : Multiply the original score by the rescore query score. Useful for function query rescores.
    avg : Average the original score and the rescore query score.
    max : Take the max of original score and the rescore query score.
    min : Take the min of the original score and the rescore query score.

*/
    public static final BoolQueryBuilder addQueryDs(BoolQueryBuilder queryBuilder
            , String datasource){
        return queryBuilder.filter(termQuery("datasource", datasource));
    }

    public static final BoolQueryBuilder addQueryLabel(BoolQueryBuilder queryBuilder
            , String label){
        return queryBuilder.filter(termQuery("label", label));
    }

    public static final BoolQueryBuilder addQueryLabels(BoolQueryBuilder queryBuilder
            , String[] labels){
        return queryBuilder.filter(termsQuery("label", labels));
    }

    public static final BoolQueryBuilder addQueryKey(BoolQueryBuilder queryBuilder, String key){
        return queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                            termQuery("properties.key", key)
                    ), ScoreMode.Total));
    }

    public static final BoolQueryBuilder addQueryKeyNot(BoolQueryBuilder queryBuilder, String key){
        return queryBuilder.mustNot(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                            termQuery("properties.key", key)
                    ), ScoreMode.Total));
    }

    public static final BoolQueryBuilder addQueryKeys(BoolQueryBuilder queryBuilder, String[] keys){
        for( String key : keys ){       // AND
            queryBuilder = queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                            termQuery("properties.key", key)
                    ), ScoreMode.Total));
        }
        return queryBuilder;
    }

    public static final BoolQueryBuilder addQueryValue(BoolQueryBuilder queryBuilder, String value){
        return queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                            QueryBuilders.queryStringQuery("properties.value:\"" + value.toLowerCase() + "\"")
                    ), ScoreMode.Total));
    }

    public static final BoolQueryBuilder addQueryValues(BoolQueryBuilder queryBuilder, String[] values){
        for( String value : values ){       // AND
            queryBuilder = queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                            QueryBuilders.queryStringQuery("properties.value:\"" + value.toLowerCase() + "\"")
                    ), ScoreMode.Total));
        }
        return queryBuilder;
    }

    public static final BoolQueryBuilder addQueryKeyValue(BoolQueryBuilder queryBuilder, String key, String value){
        return queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery()
                        .must(termQuery("properties.key", key.toLowerCase()))
                        .must(QueryBuilders.queryStringQuery("properties.value:\""+value.toLowerCase()+"\""))
                    , ScoreMode.Total));
    }

}
