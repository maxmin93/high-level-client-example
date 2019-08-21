package com.example.aws.elasticsearch.demo.elasticgraph.model;

import com.example.aws.elasticsearch.demo.basegraph.model.BaseProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

@Data
public final class ElasticProperty implements BaseProperty {

    private String key;
    private String type;
    private String value;

    public Object value(ObjectMapper mapper){
        if( value == null || value.isEmpty() ) return null;
        Object translated = (Object)value;

        try {
            Class<?> clazz = Class.forName(type);
            translated = mapper.convertValue(value, clazz);
        }
        catch (ClassNotFoundException ex){
            return translated;
        }
        return translated;
    }

}
