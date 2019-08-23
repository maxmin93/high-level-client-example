package com.example.aws.elasticsearch.demo.basegraph;

import com.example.aws.elasticsearch.demo.basegraph.model.BaseEdge;
import com.example.aws.elasticsearch.demo.basegraph.model.BaseVertex;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface BaseGraphAPI {

    enum Direction { BOTH, IN, OUT };

    BaseTx tx();

    //////////////////////////////////////////////////
    //
    // common access services about ElasticElement
    //

    Collection<BaseVertex> vertices(String datasource);
    Collection<BaseEdge> edges(String datasource);

    boolean existsVertex(String id);
    boolean existsEdge(String id);

    BaseVertex getVertexById(String id);
    BaseEdge getEdgeById(String id);

    boolean saveVertex(BaseVertex vertex);
    boolean saveEdge(BaseEdge edge);

    void dropVertex(String id);
    void dropEdge(String id);

    long countV(String datasource);
    long countE(String datasource);

    Map<String, Long> listVertexLabels(String datasource);
    Map<String, Long> listEdgeLabels(String datasource);

    Map<String, Long> listVertexLabelKeys(String datasource, String label);
    Map<String, Long> listEdgeLabelKeys(String datasource, String label);

    //////////////////////////////////////////////////
    //
    //  **NOTE: optimized find functions
    //      ==> http://tinkerpop.apache.org/docs/current/reference/#has-step
    //
    //    hasId(ids…​)              : findVertices(ids)
    //    hasLabel(labels…​)        : findVerticesWithLabels(ds, ...labels)
    //    has(key,value)           : findVerticesWithKV(ds, key, val)
    //    has(label, key, value)   : findVerticesWithLKV(ds, label, key, value)
    //    has(key)                 : findVerticesWithKey(ds, key)
    //    hasNot(key)              : findVerticesWithNotKey(ds, key)
    //    hasKey(keys…​)            : findVerticesWithKeys(ds, ...keys)
    //    hasValue(values…​)        : findVerticesWithValues(ds, ...values)

    //////////////////////////////////////////////////
    //
    // access services of Vertex
    //

    Collection<BaseVertex> findVertices(final String[] ids);
    Collection<BaseVertex> findVertices(String datasource, final String[] labels);
    Collection<BaseVertex> findVertices(String datasource, String key, String value);
    Collection<BaseVertex> findVertices(String datasource, String label, String key, String value);
    Collection<BaseVertex> findVertices(String datasource, String key, boolean hasNot);
    Collection<BaseVertex> findVerticesWithKeys(String datasource, final String[] keys);
    Collection<BaseVertex> findVerticesWithValues(String datasource, final String[] values);

    BaseVertex findOtherVertexOfEdge(String eid, String vid);
    Collection<BaseVertex> findNeighborVertices(String datasource, String vid, Direction direction, final String[] labels);

//    Collection<BaseVertex> findVertices(String datasource
//            , List<String> ids, List<String> labels, List<String> keys, List<Object> values);


    //////////////////////////////////////////////////
    //
    // access services of Edge
    //

    Collection<BaseEdge> findEdges(final String[] ids);
    Collection<BaseEdge> findEdges(String datasource, final String[] labels);
    Collection<BaseEdge> findEdges(String datasource, String key, String value);
    Collection<BaseEdge> findEdges(String datasource, String label, String key, String value);
    Collection<BaseEdge> findEdges(String datasource, String key, boolean hasNot);
    Collection<BaseEdge> findEdgesWithKeys(String datasource, final String[] keys);
    Collection<BaseEdge> findEdgesWithValues(String datasource, final String[] values);

    Collection<BaseEdge> findEdgesByDirection(String datasource, String vid, Direction direction);
    Collection<BaseEdge> findEdgesOfVertex(String datasource, String vid, Direction direction, final String[] labels);
    Collection<BaseEdge> findEdgesOfVertex(String datasource, String vid, Direction direction, String label, String key, Object value);

//    Collection<BaseEdge> findEdges(String datasource
//            , List<String> ids, List<String> labels, List<String> keys, List<Object> values);

}
