package com.example.aws.elasticsearch.demo.elasticgraph;

import com.example.aws.elasticsearch.demo.basegraph.BaseGraphAPI;
import com.example.aws.elasticsearch.demo.basegraph.BaseTx;
import com.example.aws.elasticsearch.demo.basegraph.model.BaseEdge;
import com.example.aws.elasticsearch.demo.basegraph.model.BaseVertex;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticEdge;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticVertex;
import com.example.aws.elasticsearch.demo.elasticgraph.repository.ElasticEdgeService;
import com.example.aws.elasticsearch.demo.elasticgraph.repository.ElasticGraphService;
import com.example.aws.elasticsearch.demo.elasticgraph.repository.ElasticVertexService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ElasticGraphAPI implements BaseGraphAPI {

    private final static int DEFAULT_SIZE = 2500;

    private final RestHighLevelClient client;
    private final ObjectMapper mapper;

    private final ElasticVertexService vertices;
    private final ElasticEdgeService edges;
    private final ElasticGraphService graph;

    @Autowired
    public ElasticGraphAPI(
            RestHighLevelClient client,     // elasticsearch config
            ObjectMapper mapper             // spring boot web starter
    ) {
        this.client = client;
        this.mapper = mapper;

        this.vertices = new ElasticVertexService(client, mapper);
        this.edges = new ElasticEdgeService(client, mapper);
        this.graph = new ElasticGraphService(client, mapper);
    }

    @Override
    public BaseTx tx(){
        return new BaseTx() {
            @Override public void failure() {
            }
            @Override public void success() {
            }
            @Override public void close() {
            }
        };
    }

    @PostConstruct
    private void ready() throws Exception {
        graph.ready();      // if not exists index, create index
    }

    public boolean reset() throws Exception {
        return graph.resetIndex();
    }

    public String remove(String datasource) throws Exception {
        Gson gson = new Gson();
        JsonObject object = new JsonObject();
        object.addProperty("V", vertices.deleteDocuments(datasource));
        object.addProperty("E", edges.deleteDocuments(datasource));
        return gson.toJson(object);
    }

    public String count() throws Exception {
        Gson gson = new Gson();
        JsonObject object = new JsonObject();
        object.addProperty("V", vertices.count());
        object.addProperty("E", edges.count());
        return gson.toJson(object);
    }
    public String count(String datasource) throws Exception {
        Gson gson = new Gson();
        JsonObject object = new JsonObject();
        object.addProperty("V", vertices.count(datasource));
        object.addProperty("E", edges.count(datasource));
        return gson.toJson(object);
    }
    public String labels(String datasource) throws Exception {
        Gson gson = new Gson();
        JsonElement jsonV = gson.fromJson( gson.toJson(listVertexLabels(datasource)), JsonElement.class);
        JsonElement jsonE = gson.fromJson( gson.toJson(listEdgeLabels(datasource)), JsonElement.class);

        JsonObject object = new JsonObject();
        object.add("V", jsonV);
        object.add("E", jsonE);
        return gson.toJson(object);
    }

    //////////////////////////////////////////////////
    //
    // schema services
    //


    @Override
    public Map<String, Long> listVertexLabels(String datasource) {
        try {
            return graph.listLabels(ElasticGraphService.INDEX_VERTEX, datasource);
        }
        catch (Exception e) { return Collections.EMPTY_MAP; }
    }
    @Override
    public Map<String, Long> listEdgeLabels(String datasource){
        try {
            return graph.listLabels(ElasticGraphService.INDEX_EDGE, datasource);
        }
        catch (Exception e) { return Collections.EMPTY_MAP; }
    }

    @Override
    public Map<String, Long> listVertexLabelKeys(String datasource, String label) {
        try {
            return graph.listLabelKeys(ElasticGraphService.INDEX_VERTEX, datasource, label);
        }
        catch (Exception e) { return Collections.EMPTY_MAP; }
    }
    @Override
    public Map<String, Long> listEdgeLabelKeys(String datasource, String label){
        try {
            return graph.listLabelKeys(ElasticGraphService.INDEX_EDGE, datasource, label);
        }
        catch (Exception e) { return Collections.EMPTY_MAP; }
    }

    @Override
    public long countV(String datasource){
        try{ return vertices.count(datasource); }
        catch(Exception e){ return -1L; }
    }
    @Override
    public long countE(String datasource){
        try{ return edges.count(datasource); }
        catch(Exception e){ return -1L; }
    }

    public long countV() {
        try{ return vertices.count(); }
        catch(Exception e){ return -1L; }
    }
    public long countE() {
        try{ return edges.count(); }
        catch(Exception e){ return -1L; }
    }

    //////////////////////////////////////////////////
    //
    // common access services about ElasticElement
    //

    @Override
    public Collection<BaseVertex> vertices(String datasource){
        try{
            return vertices.findByDatasource(DEFAULT_SIZE, datasource)
                    .stream().map(r->(BaseVertex)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }
    @Override
    public Collection<BaseEdge> edges(String datasource){
        try {
            return edges.findByDatasource(DEFAULT_SIZE, datasource)
                    .stream().map(r->(BaseEdge)r).collect(Collectors.toList());
        }
        catch (Exception e){ return Collections.EMPTY_LIST; }
    }

    @Override
    public boolean existsVertex(String id){
        try{ return vertices.existsId(id); }
        catch(Exception e){ return false; }
    }
    @Override
    public boolean existsEdge(String id){
        try{ return edges.existsId(id); }
        catch(Exception e){ return false; }
    }

    @Override
    public BaseVertex getVertexById(String id){
        try{ return vertices.findById(id); }
        catch(Exception e){ return null; }
    }
    @Override
    public BaseEdge getEdgeById(String id){
        try{ return edges.findById(id); }
        catch(Exception e){ return null; }
    }

    @Override
    public boolean saveVertex(BaseVertex vertex){
        try{
            if( existsVertex(vertex.getId()) )
                return vertices.updateDocument((ElasticVertex) vertex).equals("UPDATED") ? true : false;
            else
                return vertices.createDocument((ElasticVertex) vertex).equals("CREATED") ? true : false;
        }
        catch(Exception e){ return false; }
    }
    @Override
    public boolean saveEdge(BaseEdge edge){
        try{
            if( existsEdge(edge.getId()) )
                return edges.updateDocument((ElasticEdge) edge).equals("UPDATED") ? true : false;
            else
                return edges.createDocument((ElasticEdge) edge).equals("CREATED") ? true : false;
        }
        catch(Exception e){ return false; }
    }

    @Override
    public void dropVertex(String id){
        try{ vertices.deleteDocument(id); }
        catch (Exception e){ }
    }
    @Override
    public void dropEdge(String id){
        try{ edges.deleteDocument(id); }
        catch (Exception e){ }
    }

    ///////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////
    //  **NOTE: optimized find functions
    //      ==> http://tinkerpop.apache.org/docs/current/reference/#has-step
    //    has(key,value)          AND : findVerticesWithKV(ds, key, val)
    //    has(label, key, value)  AND : findVerticesWithLKV(ds, label, key, value)
    //    hasLabel(labels…​)       OR  : findVerticesWithLabels(ds, ...labels)
    //    hasId(ids…​)             OR  : findVertices(ids)
    //    hasKey(keys…​)           AND : findVerticesWithKeys(ds, ...keys)
    //    hasValue(values…​)       AND : findVerticesWithValues(ds, ...values)
    //    has(key)                EQ  : findVerticesWithKey(ds, key)
    //    hasNot(key)             NEQ : findVerticesWithNotKey(ds, key)

    ///////////////////////////////////////////////////////////////
    // find vertices for baseAPI

    @Override
    public Collection<BaseVertex> findVertices(final String[] ids){
        try{
            return vertices.findByIds(ids)
                    .stream().map(r->(BaseVertex)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }
    @Override
    public Collection<BaseVertex> findVertices(String datasource, final String[] labels){
        try{
            return vertices.findByDatasourceAndLabels(DEFAULT_SIZE, datasource, labels)
                    .stream().map(r->(BaseVertex)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }
    @Override
    public Collection<BaseVertex> findVertices(String datasource, String key, String value){
        try{
            return vertices.findByDatasourceAndPropertyKeyValue(DEFAULT_SIZE, datasource, key, value)
                    .stream().map(r->(BaseVertex)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }
    @Override
    public Collection<BaseVertex> findVertices(String datasource, String label, String key, String value){
        try{
            return vertices.findByDatasourceAndLabelAndPropertyKeyValue(DEFAULT_SIZE, datasource, label, key, value)
                    .stream().map(r->(BaseVertex)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }
    @Override
    public Collection<BaseVertex> findVertices(String datasource, String key, boolean hasNot){
        try{
            Collection<ElasticVertex> list = !hasNot
                    ? vertices.findByDatasourceAndPropertyKey(DEFAULT_SIZE, datasource, key)
                    : vertices.findByDatasourceAndPropertyKeyNot(DEFAULT_SIZE, datasource, key);
            return list.stream().map(r->(BaseVertex)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }
    @Override
    public Collection<BaseVertex> findVerticesWithKeys(String datasource, final String[] keys){
        try{
            return vertices.findByDatasourceAndPropertyKeys(DEFAULT_SIZE, datasource, keys)
                    .stream().map(r->(BaseVertex)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }
    @Override
    public Collection<BaseVertex> findVerticesWithValues(String datasource, final String[] values){
        try{
            return vertices.findByDatasourceAndPropertyValues(DEFAULT_SIZE, datasource, values)
                    .stream().map(r->(BaseVertex)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }

    @Override
    public BaseVertex findOtherVertexOfEdge(String eid, String vid){
        try{
            ElasticEdge edge = edges.findById(eid);
            if( edge == null ) return null;
            String otherVid = vid.equals(edge.getSid()) ? edge.getTid() : edge.getSid();
            return vertices.findById(otherVid);
        }
        catch(Exception e){ return null; }
    }

    @Override
    public Collection<BaseVertex> findNeighborVertices(String datasource, String vid, Direction direction, String[] labels){
        try{
            Collection<ElasticEdge> links = edges.findByDatasourceAndDirection(DEFAULT_SIZE, datasource, vid, direction);
            Set<String> neighborIds = links.stream()
                    .map(r->r.getSid().equals(vid) ? r.getTid() : r.getSid()).collect(Collectors.toSet());

            String[] arrayIds = new String[neighborIds.size()];
            if( labels.length > 0 ){
                List<String> filterLabels = Arrays.asList(labels);
                return vertices.findByIds(neighborIds.toArray(arrayIds)).stream()
                        .filter(r->filterLabels.contains(r.getLabel()))
                        .map(r->(BaseVertex)r).collect(Collectors.toList());
            }
            else
                return vertices.findByIds(neighborIds.toArray(arrayIds))
                        .stream().map(r->(BaseVertex)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }

    ///////////////////////////////////////////////////////////////
    // find vertices for native

    // V.hasId : ids
    public List<ElasticVertex> findV_Ids(String[] ids) throws Exception {
        return vertices.findByIds(ids);
    }
    // V : datasource
    public List<ElasticVertex> findV_Datasource(int size, String datasource) throws Exception {
        return vertices.findByDatasource(size, datasource);
    }
    // V : datasource + labels
    public List<ElasticVertex> findV_DatasourceAndLabels(int size, String datasource, String[] labels) throws Exception {
        return vertices.findByDatasourceAndLabels(size, datasource, labels);
    }
    // V : datasource + (property.key)
    public List<ElasticVertex> findV_DatasourceAndPropertyKey(int size, String datasource, String key) throws Exception {
        return vertices.findByDatasourceAndPropertyKey(size, datasource, key);
    }
    // V : datasource + (NOT property.key)
    public List<ElasticVertex> findV_DatasourceAndPropertyKeyNot(int size, String datasource, String key) throws Exception {
        return vertices.findByDatasourceAndPropertyKeyNot(size, datasource, key);
    }
    // V : datasource + property.keys
    public List<ElasticVertex> findV_DatasourceAndPropertyKeys(int size, String datasource, String[] keys) throws Exception {
        return vertices.findByDatasourceAndPropertyKeys(size, datasource, keys);
    }
    // V : datasource + property.values
    public List<ElasticVertex> findV_DatasourceAndPropertyValues(int size, String datasource, String[] values) throws Exception {
        return vertices.findByDatasourceAndPropertyValues(size, datasource, values);
    }
    // V : datasource + property.values (partial)
    public List<ElasticVertex> findV_DatasourceAndPropertyValuePartial(String datasource, String value) throws Exception {
        return vertices.findByDatasourceAndPropertyValuePartial(DEFAULT_SIZE, datasource, value);
    }
    // V : datasource + (property.key & .value)
    public List<ElasticVertex> findV_DatasourceAndPropertyKeyValue(int size, String datasource, String key, String value) throws Exception {
        return vertices.findByDatasourceAndPropertyKeyValue(size, datasource, key, value);
    }
    // V.has(label,key,value) : datasource + label + (property.key & .value)
    public List<ElasticVertex> findV_DatasourceAndLabelAndPropertyKeyValue(int size, String datasource, String label, String key, String value) throws Exception {
        return vertices.findByDatasourceAndLabelAndPropertyKeyValue(size, datasource, label, key, value);
    }

    ///////////////////////////////////////////////////////////////
    // find edges for baseAPI

    @Override
    public Collection<BaseEdge> findEdges(final String[] ids){
        try{
            return edges.findByIds(ids)
                    .stream().map(r->(BaseEdge)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }
    @Override
    public Collection<BaseEdge> findEdges(String datasource, final String[] labels){
        try{
            return edges.findByDatasourceAndLabels(DEFAULT_SIZE, datasource, labels)
                    .stream().map(r->(BaseEdge)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }
    @Override
    public Collection<BaseEdge> findEdges(String datasource, String key, String value){
        try{
            return edges.findByDatasourceAndPropertyKeyValue(DEFAULT_SIZE, datasource, key, value)
                    .stream().map(r->(BaseEdge)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }
    @Override
    public Collection<BaseEdge> findEdges(String datasource, String label, String key, String value){
        try{
            return edges.findByDatasourceAndLabelAndPropertyKeyValue(DEFAULT_SIZE, datasource, label, key, value)
                    .stream().map(r->(BaseEdge)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }
    @Override
    public Collection<BaseEdge> findEdges(String datasource, String key, boolean hasNot){
        try{
            Collection<ElasticEdge> list = !hasNot
                    ? edges.findByDatasourceAndPropertyKey(DEFAULT_SIZE, datasource, key)
                    : edges.findByDatasourceAndPropertyKeyNot(DEFAULT_SIZE, datasource, key);
            return list.stream().map(r->(BaseEdge)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }
    @Override
    public Collection<BaseEdge> findEdgesWithKeys(String datasource, final String[] keys){
        try{
            return edges.findByDatasourceAndPropertyKeys(DEFAULT_SIZE, datasource, keys)
                    .stream().map(r->(BaseEdge)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }
    @Override
    public Collection<BaseEdge> findEdgesWithValues(String datasource, final String[] values){
        try{
            return edges.findByDatasourceAndPropertyValues(DEFAULT_SIZE, datasource, values)
                    .stream().map(r->(BaseEdge)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }

    @Override
    public Collection<BaseEdge> findEdgesByDirection(String datasource, String vid, Direction direction){
        try{
            return edges.findByDatasourceAndDirection(DEFAULT_SIZE, datasource, vid, direction)
                    .stream().map(r->(BaseEdge)r).collect(Collectors.toList());
        }
        catch(Exception e){ return Collections.EMPTY_LIST; }
    }

    @Override
    public Collection<BaseEdge> findEdgesOfVertex(String datasource, String vid, Direction direction, final String[] labels){
        if( labels.length > 0 ){
            List<String> filterLabels = Arrays.asList(labels);
            return findEdgesByDirection(datasource, vid, direction).stream()
                    .filter(r->filterLabels.contains(r.getLabel())).collect(Collectors.toList());
        }
        return findEdgesByDirection(datasource, vid, direction);
    }

    @Override
    public Collection<BaseEdge> findEdgesOfVertex(String datasource, String vid, Direction direction, String label, String key, Object value){
        return findEdgesByDirection(datasource, vid, direction).stream()
                .filter(r->{
                    if( label != null && !label.equals(r.getLabel()) ) return false;
                    if( key != null ){
                        if( !r.keys().contains(key) ) return false;
                        if( value != null && !r.getProperty(key).value().equals(value) ) return false;
                    }
                    return true;
                }).collect(Collectors.toList());
    }

    ///////////////////////////////////////////////////////////////
    // find edges for native

    // E.hasId : ids
    public List<ElasticEdge> findE_Ids(String[] ids) throws Exception {
        return edges.findByIds(ids);
    }
    // E : datasource
    public List<ElasticEdge> findE_Datasource(int size, String datasource) throws Exception {
        return edges.findByDatasource(size, datasource);
    }
    // E.hasLabel : datasource + labels
    public List<ElasticEdge> findE_DatasourceAndLabels(int size, String datasource, String[] labels) throws Exception {
        return edges.findByDatasourceAndLabels(size, datasource, labels);
    }
    // E : datasource + property.keys
    public List<ElasticEdge> findE_DatasourceAndPropertyKeys(int size, String datasource, String[] keys) throws Exception {
        return edges.findByDatasourceAndPropertyKeys(size, datasource, keys);
    }
    // E : datasource + (NOT property.key)
    public List<ElasticEdge> findE_DatasourceAndPropertyKeyNot(String datasource, String key) throws Exception {
        return edges.findByDatasourceAndPropertyKeyNot(DEFAULT_SIZE, datasource, key);
    }
    // E : datasource + (property.key)
    public List<ElasticEdge> findE_DatasourceAndPropertyKey(int size, String datasource, String key) throws Exception {
        return edges.findByDatasourceAndPropertyKey(size, datasource, key);
    }
    // E : datasource + property.values
    public List<ElasticEdge> findE_DatasourceAndPropertyValues(int size, String datasource, String[] values) throws Exception {
        return edges.findByDatasourceAndPropertyValues(size, datasource, values);
    }
    // E : datasource + property.values (partial)
    public List<ElasticEdge> findE_DatasourceAndPropertyValuePartial(String datasource, String value) throws Exception {
        return edges.findByDatasourceAndPropertyValuePartial(DEFAULT_SIZE, datasource, value);
    }
    // E.has(key,value) : datasource + (property.key & .value)
    public List<ElasticEdge> findE_DatasourceAndPropertyKeyValue(int size, String datasource, String key, String value) throws Exception {
        return edges.findByDatasourceAndPropertyKeyValue(size, datasource, key, value);
    }
    // E.has(label,key,value) : datasource + label + (property.key & .value)
    public List<ElasticEdge> findE_DatasourceAndLabelAndPropertyKeyValue(int size, String datasource, String label, String key, String value) throws Exception {
        return edges.findByDatasourceAndLabelAndPropertyKeyValue(size, datasource, label, key, value);
    }

}
