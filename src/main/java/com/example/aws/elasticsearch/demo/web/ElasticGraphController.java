package com.example.aws.elasticsearch.demo.web;

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
    */
    @PostMapping("/v")
    public ResponseEntity addVertex(@RequestBody ElasticVertex document) throws Exception {
        return new ResponseEntity(base.addV(document), HttpStatus.CREATED);
    }
    @PostMapping("/e")
    public ResponseEntity addEdge(@RequestBody ElasticEdge document) throws Exception {
        return new ResponseEntity(base.addE(document), HttpStatus.CREATED);
    }

    /*
curl -X PUT -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"v02", "label":"person", "datasource": "sample", "properties": [ {"key":"technology", "type": "java.lang.String", "value":"typescript"}, {"key":"years_of_experience", "type": "java.lang.Integer", "value":"2"}, {"key":"gpa", "type": "java.lang.Float", "value":"3.7"}] }' localhost:8080/elastic/v
curl -X PUT -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"e02", "label":"knows", "datasource": "sample", "sid":"v02", "tid":"v03", "properties": [ {"key":"year", "type": "java.lang.String", "value":"2003"}] }' localhost:8080/elastic/e
    */
    @PutMapping("/v")
    public ResponseEntity updateVertex(@RequestBody ElasticVertex document) throws Exception {
        return new ResponseEntity(base.updateV(document), HttpStatus.CREATED);
    }
    @PutMapping("/e")
    public ResponseEntity updateEdge(@RequestBody ElasticEdge document) throws Exception {
        return new ResponseEntity(base.updateE(document), HttpStatus.CREATED);
    }

    /*
curl -X DELETE "localhost:8080/elastic/v/v04"
==> 자동으로 연결된 간선들[e03]도 제거 되어야 함 (cascade)
    */
    @DeleteMapping("/v/{id}")
    public String removeVertex(@PathVariable String id) throws Exception {
        return base.removeV(id);
    }
    @DeleteMapping("/e/{id}")
    public String removeEdge(@PathVariable String id) throws Exception {
        return base.removeE(id);
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
    public ElasticVertex findV(@PathVariable String id) throws Exception {
        ElasticVertex d = base.findV(id);
        for(ElasticProperty p : d.getProperties()){
            System.out.println(p.getKey() +" = "+p.value(mapper).toString() );
        }
        return d;
    }
    @GetMapping("/e/{id}")
    public ElasticEdge findE(@PathVariable String id) throws Exception {
        return base.findE(id);
    }

    /*
curl -X GET "localhost:8080/elastic/sample/v"
curl -X GET "localhost:8080/elastic/sample/e"
    */
    @GetMapping("/{datasource}/v")
    public List<ElasticVertex> findV_All(@PathVariable String datasource) throws Exception {
        return base.findV_Datasource(datasource);
    }
    @GetMapping("/{datasource}/e")
    public List<ElasticEdge> findE_All(@PathVariable String datasource) throws Exception {
        return base.findE_Datasource(datasource);
    }

    ///////////////////////////////////////////////////////////////

    /*
curl -X GET "localhost:8080/elastic/sample/v/label?q=person"
curl -X GET "localhost:8080/elastic/sample/e/label?q=person"
    */
    @GetMapping(value = "/{datasource}/v/labels")
    public List<ElasticVertex> findV_Label(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> labels
    ) throws Exception {
        String[] array = new String[labels.size()];
        return base.findV_DatasourceAndLabels(datasource, labels.toArray(array));
    }
    @GetMapping(value = "/{datasource}/e/labels")
    public List<ElasticEdge> findE_Label(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> labels
    ) throws Exception {
        String[] array = new String[labels.size()];
        return base.findE_DatasourceAndLabels(datasource, labels.toArray(array));
    }


    @GetMapping(value = "/{datasource}/v/keys")
    public List<ElasticVertex> findV_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> keys
    ) throws Exception {
        String[] array = new String[keys.size()];
        return base.findV_DatasourceAndPropertyKeys(datasource, keys.toArray(array));
    }
    @GetMapping(value = "/{datasource}/e/keys")
    public List<ElasticEdge> findE_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> keys
    ) throws Exception {
        String[] array = new String[keys.size()];
        return base.findE_DatasourceAndPropertyKeys(datasource, keys.toArray(array));
    }


    @GetMapping(value = "/{datasource}/v/notkey")
    public List<ElasticVertex> findV_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") String key
    ) throws Exception {
        return base.findV_DatasourceAndPropertyKeyNot(datasource, key);
    }
    @GetMapping(value = "/{datasource}/e/notkey")
    public List<ElasticEdge> findE_PropertyKey(
            @PathVariable String datasource,
            @RequestParam(value = "q") String key
    ) throws Exception {
        return base.findE_DatasourceAndPropertyKeyNot(datasource, key);
    }


    @GetMapping(value = "/{datasource}/v/values")
    public List<ElasticVertex> findV_PropertyValues(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> values
    ) throws Exception {
        String[] array = new String[values.size()];
        return base.findV_DatasourceAndPropertyValues(datasource, values.toArray(array));
    }
    @GetMapping(value = "/{datasource}/e/values")
    public List<ElasticEdge> findE_PropertyValues(
            @PathVariable String datasource,
            @RequestParam(value = "q") List<String> values
    ) throws Exception {
        String[] array = new String[values.size()];
        return base.findE_DatasourceAndPropertyValues(datasource, values.toArray(array));
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
    public List<ElasticVertex> findV_PropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return base.findV_DatasourceAndPropertyKeyValue(datasource, key, value);
    }
    @GetMapping(value = "/{datasource}/e/keyvalue")
    public List<ElasticEdge> findE_PropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return base.findE_DatasourceAndPropertyKeyValue(datasource, key, value);
    }


    @GetMapping(value = "/{datasource}/v/labelkeyvalue")
    public List<ElasticVertex> findV_LabelAndPropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "label") String label,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return base.findV_DatasourceAndLabelAndPropertyKeyValue(datasource, label, key, value);
    }
    @GetMapping(value = "/{datasource}/e/labelkeyvalue")
    public List<ElasticEdge> findE_LabelAndPropertyKeyValue(
            @PathVariable String datasource,
            @RequestParam(value = "label") String label,
            @RequestParam(value = "key") String key,
            @RequestParam(value = "value") String value
    ) throws Exception {
        return base.findE_DatasourceAndLabelAndPropertyKeyValue(datasource, label, key, value);
    }

}
