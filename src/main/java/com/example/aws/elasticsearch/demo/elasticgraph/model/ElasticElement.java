package com.example.aws.elasticsearch.demo.elasticgraph.model;

import com.example.aws.elasticsearch.demo.basegraph.model.BaseElement;
import com.example.aws.elasticsearch.demo.basegraph.model.BaseProperty;
import lombok.Data;

import java.util.*;
import java.util.stream.Collectors;

@Data
public class ElasticElement implements BaseElement {

    protected String id;
    protected String label;
    protected String datasource;
    protected List<ElasticProperty> properties;

    @Override
    public List<String> getKeys(){
        return properties.stream().map(r->r.getKey()).collect(Collectors.toList());
    }

    @Override
    public List<Object> getValues(){
        return properties.stream().map(r->r.value()).collect(Collectors.toList());
    }

    @Override
    public Collection<BaseProperty> properties(){
        return new HashSet<>(properties);
    }

    @Override
    public void properties(Collection<? extends BaseProperty> properties){
        this.properties = properties.stream().map(r->(ElasticProperty)r).collect(Collectors.toList());
    }

    @Override
    public boolean exists(String key){
        return getKeys().contains(key);
    }

    @Override
    public BaseProperty getProperty(String key){
        for( ElasticProperty property : properties ){
            if( property.getKey().equals(key) ) return property;
        }
        return null;
    }

    @Override
    public BaseProperty getProperty(String key, BaseProperty defaultProperty){
        BaseProperty property = getProperty(key);
        return property == null ? defaultProperty : property;
    }

    @Override
    public void setProperty(BaseProperty property){
        if( exists(property.key()) ) removeProperty(property.key());
        properties.add((ElasticProperty) property);
    }

    @Override
    public BaseProperty removeProperty(String key){
        Iterator<ElasticProperty> iter = properties.iterator();
        while(iter.hasNext()){
            BaseProperty property = iter.next();
            if( property.key().equals(key) ){
                iter.remove();
                return property;
            }
        }
        return null;
    }
}
