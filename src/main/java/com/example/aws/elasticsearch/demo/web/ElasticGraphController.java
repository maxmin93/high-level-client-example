package com.example.aws.elasticsearch.demo.web;

import com.example.aws.elasticsearch.demo.basegraph.model.BaseEdge;
import com.example.aws.elasticsearch.demo.basegraph.model.BaseProperty;
import com.example.aws.elasticsearch.demo.basegraph.model.BaseVertex;
import com.example.aws.elasticsearch.demo.elasticgraph.ElasticGraphAPI;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticEdge;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticProperty;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticVertex;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/elastic")
public class ElasticGraphController {

    private final ElasticGraphAPI base;
    private final ObjectMapper mapper;

    @Autowired
    public ElasticGraphController(ElasticGraphAPI base, ObjectMapper mapper){
        this.base = base;
        this.mapper = mapper;
    }

    @GetMapping("/test")
    public String test(){
        return "Success";
    }

    ///////////////////////////////////////////////////////////////

    /*
curl -X PUT "localhost:8080/elastic/reset"
    */
    @PutMapping("/reset")
    public ResponseEntity resetIndex() throws Exception {
        return new ResponseEntity(base.reset(), HttpStatus.OK);
    }

    @GetMapping("/count")
    public ResponseEntity count() throws Exception {
        return new ResponseEntity(base.count(), HttpStatus.OK);
    }
    @GetMapping("/{datasource}/count")
    public ResponseEntity count(@PathVariable String datasource) throws Exception {
        return new ResponseEntity(base.count(datasource), HttpStatus.OK);
    }

    ///////////////////////////////////////////////////////////////

    /*
curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"v01", "label":"person", "datasource": "sample", "properties": [ {"key":"technology", "type": "java.lang.String", "value":"java"}, {"key":"years_of_experience", "type": "java.lang.Integer", "value":"5"}] }' localhost:8080/elastic/v
curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"v02", "label":"person", "datasource": "sample", "properties": [ {"key":"technology", "type": "java.lang.String", "value":"typescript"}, {"key":"years_of_experience", "type": "java.lang.Integer", "value":"3"}] }' localhost:8080/elastic/v
curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"v03", "label":"person", "datasource": "sample", "properties": [ {"key":"technology", "type": "java.lang.String", "value":"html5/css"}, {"key":"years_of_experience", "type": "java.lang.Integer", "value":"4"}] }' localhost:8080/elastic/v
curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"v04", "label":"person", "datasource": "sample", "properties": [ {"key":"technology", "type": "java.lang.String", "value":"python"}, {"key":"years_of_experience", "type": "java.lang.Integer", "value":"6"}] }' localhost:8080/elastic/v
curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"e01", "label":"knows", "datasource": "sample", "sid":"v01", "tid":"v02", "properties": [ {"key":"year", "type": "java.lang.String", "value":"2001"}, {"key":"relation", "type": "java.lang.String", "value":"friend"}] }' localhost:8080/elastic/e
curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"e02", "label":"knows", "datasource": "sample", "sid":"v02", "tid":"v03", "properties": [ {"key":"year", "type": "java.lang.String", "value":"2003"}, {"key":"relation", "type": "java.lang.String", "value":"crew"}] }' localhost:8080/elastic/e
curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"e03", "label":"knows", "datasource": "sample", "sid":"v02", "tid":"v03", "properties": [ {"key":"year", "type": "java.lang.String", "value":"1982"}, {"key":"relation", "type": "java.lang.String", "value":"family"}] }' localhost:8080/elastic/e

curl -X PUT -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"v02", "label":"person", "datasource": "sample", "properties": [ {"key":"technology", "type": "java.lang.String", "value":"typescript"}, {"key":"years_of_experience", "type": "java.lang.Integer", "value":"2"}, {"key":"gpa", "type": "java.lang.Float", "value":"3.7"}] }' localhost:8080/elastic/v
curl -X PUT -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"e02", "label":"knows", "datasource": "sample", "sid":"v02", "tid":"v03", "properties": [ {"key":"year", "type": "java.lang.String", "value":"2003"}] }' localhost:8080/elastic/e
    */
    @PostMapping("/v")
    @PutMapping("/v")
    public ResponseEntity saveVertex(@RequestBody ElasticVertex document) throws Exception {
        return new ResponseEntity(base.saveVertex(document), HttpStatus.CREATED);
    }
    @PostMapping("/e")
    @PutMapping("/e")
    public ResponseEntity saveEdge(@RequestBody ElasticEdge document) throws Exception {
        return new ResponseEntity(base.saveEdge(document), HttpStatus.CREATED);
    }

     /*
curl -X DELETE "localhost:8080/elastic/v/v04"
==> 자동으로 연결된 간선들[e03]도 제거 되어야 함 (cascade)
    */
    @DeleteMapping("/v/{id}")
    public ResponseEntity dropVertex(@PathVariable String id) throws Exception {
        base.dropVertex(id);
        return new ResponseEntity(true, HttpStatus.OK);
    }
    @DeleteMapping("/e/{id}")
    public ResponseEntity dropEdge(@PathVariable String id) throws Exception {
        base.dropEdge(id);
        return new ResponseEntity(true, HttpStatus.OK);
    }

    @DeleteMapping("/{datasource}")
    public ResponseEntity remove(@PathVariable String datasource) throws Exception {
        return new ResponseEntity(base.remove(datasource), HttpStatus.OK);
    }

    ///////////////////////////////////////////////////////////////

    /*
curl -X GET "localhost:8080/elastic/v/v01"
curl -X GET "localhost:8080/elastic/e/e01"
    */
    @GetMapping("/v/{id}")
    public BaseVertex findV(@PathVariable String id) throws Exception {
        BaseVertex d = base.getVertexById(id);
        for(BaseProperty p : d.properties()){
            System.out.println(p.key() +" = "+p.value().toString() );
        }
        return d;
    }
    @GetMapping("/e/{id}")
    public BaseEdge findE(@PathVariable String id) throws Exception {
        return base.getEdgeById(id);
    }

    /*
curl -X GET "localhost:8080/elastic/sample/v"
curl -X GET "localhost:8080/elastic/sample/e"
    */
    @GetMapping("/{datasource}/v")
    public Collection<BaseVertex> findV_All(@PathVariable String datasource) throws Exception {
        return base.vertices(datasource);
    }
    @GetMapping("/{datasource}/e")
    public Collection<BaseEdge> findE_All(@PathVariable String datasource) throws Exception {
        return base.edges(datasource);
    }

    ///////////////////////////////////////////////////////////////

    /*
curl -X GET "localhost:8080/elastic/sample/v/label?q=person"
curl -X GET "localhost:8080/elastic/sample/e/label?q=person"
    */
    @GetMapping(value = "/{datasource}/v/labels")
    public Collection<BaseVertex> findV_Label(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> labels
    ) throws Exception {
        String[] array = new String[labels.size()];
        return base.findVertices(datasource, labels.toArray(array));
    }
    @GetMapping(value = "/{datasource}/e/labels")
    public Collection<BaseEdge> findE_Label(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> labels
    ) throws Exception {
        String[] array = new String[labels.size()];
        return base.findEdges(datasource, labels.toArray(array));
    }


    @GetMapping(value = "/{datasource}/v/keys")
    public Collection<BaseVertex> findV_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> keys
    ) throws Exception {
        String[] array = new String[keys.size()];
        return base.findVerticesWithKeys(datasource, keys.toArray(array));
    }
    @GetMapping(value = "/{datasource}/e/keys")
    public Collection<BaseEdge> findE_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> keys
    ) throws Exception {
        String[] array = new String[keys.size()];
        return base.findEdgesWithKeys(datasource, keys.toArray(array));
    }


    @GetMapping(value = "/{datasource}/v/key")
    public Collection<BaseVertex> findV_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") String key,
            @RequestParam(value = "hasNot", required=false, defaultValue="true") boolean hasNot
    ) throws Exception {
        return base.findVertices(datasource, key, hasNot);
    }
    @GetMapping(value = "/{datasource}/e/key")
    public Collection<BaseEdge> findE_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") String key,
            @RequestParam(value = "hasNot", required=false, defaultValue="true") boolean hasNot
    ) throws Exception {
        return base.findEdges(datasource, key, hasNot);
    }


    @GetMapping(value = "/{datasource}/v/values")
    public Collection<BaseVertex> findV_PropertyValues(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> values
    ) throws Exception {
        String[] array = new String[values.size()];
        return base.findVerticesWithValues(datasource, values.toArray(array));
    }
    @GetMapping(value = "/{datasource}/e/values")
    public Collection<BaseEdge> findE_PropertyValues(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> values
    ) throws Exception {
        String[] array = new String[values.size()];
        return base.findEdgesWithValues(datasource, values.toArray(array));
    }


    @GetMapping(value = "/{datasource}/v/value")
    public List<ElasticVertex> findV_PropertyValuePartial(
            @PathVariable String datasource,
            @RequestParam(value = "q") String value
    ) throws Exception {
        return base.findV_DatasourceAndPropertyValuePartial(datasource, value);
    }
    @GetMapping(value = "/{datasource}/e/value")
    public List<ElasticEdge> findE_PropertyValuePartial(
            @PathVariable String datasource,
            @RequestParam(value = "q") String value
    ) throws Exception {
        return base.findE_DatasourceAndPropertyValuePartial(datasource, value);
    }

    ////////////////////////////////////////////////


    @GetMapping(value = "/{datasource}/v/keyvalue")
    public Collection<BaseVertex> findV_PropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return base.findVertices(datasource, key, value);
    }
    @GetMapping(value = "/{datasource}/e/keyvalue")
    public Collection<BaseEdge> findE_PropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return base.findEdges(datasource, key, value);
    }


    @GetMapping(value = "/{datasource}/v/labelkeyvalue")
    public Collection<BaseVertex> findV_LabelAndPropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "label") String label,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return base.findVertices(datasource, label, key, value);
    }
    @GetMapping(value = "/{datasource}/e/labelkeyvalue")
    public Collection<BaseEdge> findE_LabelAndPropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "label") String label,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return base.findEdges(datasource, label, key, value);
    }

}
