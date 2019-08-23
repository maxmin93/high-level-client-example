package com.example.aws.elasticsearch.demo.elasticgraph.model;

import com.example.aws.elasticsearch.demo.basegraph.model.BaseProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.util.NoSuchElementException;

@Data
public final class ElasticProperty implements BaseProperty {

    private String key;
    private String type;
    private String value;

    @Override
    public String key(){ return key; }

    @Override
    public String type(){ return type; }

    @Override
    public Object value() throws NoSuchElementException {
        ObjectMapper mapper = new ObjectMapper();
        if( value == null || value.isEmpty() || mapper == null ){
            throw new NoSuchElementException("ElasticProperty");
        }

        Object translated = (Object)value;
        try {
            Class<?> clazz = Class.forName(type);
            translated = mapper.convertValue(value, clazz);
        }
        catch (ClassNotFoundException ex){
            return null;
        }
        return translated;
    }

}
