package com.example.aws.elasticsearch.demo.basegraph.model;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface BaseElement {

    String getId();
    String getLabel();
    String getDatasource();

    List<String> getKeys();
    List<Object> getValues();

    boolean exists(String key);

    Collection<BaseProperty> properties();
    void properties(Collection<? extends BaseProperty> properties);

    BaseProperty getProperty(String key);
    BaseProperty getProperty(String key, BaseProperty defaultProperty);

    // upsert
    void setProperty(BaseProperty property);
    // delete
    BaseProperty removeProperty(String key);

}
