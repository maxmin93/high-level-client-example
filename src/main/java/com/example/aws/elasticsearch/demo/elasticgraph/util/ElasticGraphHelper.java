package com.example.aws.elasticsearch.demo.elasticgraph.util;

import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticEdge;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticElement;
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

}
