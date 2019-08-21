package com.example.aws.elasticsearch.demo.controller;

import com.example.aws.elasticsearch.demo.elasticgraph.ElasticGraphAPI;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticEdgeDocument;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticProperty;
import com.example.aws.elasticsearch.demo.elasticgraph.model.ElasticVertexDocument;
import com.example.aws.elasticsearch.demo.elasticgraph.repository.ElasticGraphService;
import com.example.aws.elasticsearch.demo.profilesample.model.ProfileDocument;
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
    public ResponseEntity addVertex(@RequestBody ElasticVertexDocument document) throws Exception {
        return new ResponseEntity(base.addVertex(document), HttpStatus.CREATED);
    }
    @PostMapping("/e")
    public ResponseEntity addEdge(@RequestBody ElasticEdgeDocument document) throws Exception {
        return new ResponseEntity(base.addEdge(document), HttpStatus.CREATED);
    }

    /*
curl -X PUT -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"v02", "label":"person", "datasource": "sample", "properties": [ {"key":"technology", "type": "java.lang.String", "value":"typescript"}, {"key":"years_of_experience", "type": "java.lang.Integer", "value":"2"}, {"key":"gpa", "type": "java.lang.Float", "value":"3.7"}] }' localhost:8080/elastic/v
curl -X PUT -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"e02", "label":"knows", "datasource": "sample", "sid":"v02", "tid","v03", "properties": [ {"key":"year", "type": "java.lang.String", "value":"2003"}] }' localhost:8080/elastic/e
    */
    @PutMapping("/v")
    public ResponseEntity updateVertex(@RequestBody ElasticVertexDocument document) throws Exception {
        return new ResponseEntity(base.updateVertex(document), HttpStatus.CREATED);
    }
    @PutMapping("/e")
    public ResponseEntity updateEdge(@RequestBody ElasticEdgeDocument document) throws Exception {
        return new ResponseEntity(base.updateEdge(document), HttpStatus.CREATED);
    }

    /*
curl -X DELETE "localhost:8080/elastic/v/v04"
==> 자동으로 연결된 간선들[e03]도 제거 되어야 함 (cascade)
    */
    @DeleteMapping("/v/{id}")
    public String removeVertex(@PathVariable String id) throws Exception {
        return base.removeVertex(id);
    }
    @DeleteMapping("/e/{id}")
    public String removeEdge(@PathVariable String id) throws Exception {
        return base.removeEdge(id);
    }

    ///////////////////////////////////////////////////////////////

    /*
curl -X GET "localhost:8080/elastic/v/v01"
curl -X GET "localhost:8080/elastic/e/e01"
    */
    @GetMapping("/v/{id}")
    public ElasticVertexDocument findVertex(@PathVariable String id) throws Exception {
        ElasticVertexDocument d = base.findVertex(id);
        for(ElasticProperty p : d.getProperties()){
            System.out.println(p.getKey() +" = "+p.value(mapper).toString() );
        }
        return d;
    }
    @GetMapping("/e/{id}")
    public ElasticEdgeDocument findEdge(@PathVariable String id) throws Exception {
        return base.findEdge(id);
    }

    /*
curl -X GET "localhost:8080/elastic/sample/v"
curl -X GET "localhost:8080/elastic/sample/e"
    */
    @GetMapping("/{datasource}/v")
    public List<ElasticVertexDocument> findVertices(@PathVariable String datasource) throws Exception {
        return base.findVerticesByDatasource(datasource);
    }
    @GetMapping("/{datasource}/e")
    public List<ElasticEdgeDocument> findEdges(@PathVariable String datasource) throws Exception {
        return base.findEdgesByDatasource(datasource);
    }

    ///////////////////////////////////////////////////////////////

    /*
curl -X GET "localhost:8080/elastic/sample/v/label?q=person"
curl -X GET "localhost:8080/elastic/sample/e/label?q=person"
    */
    @GetMapping(value = "/{datasource}/v/label")
    public List<ElasticVertexDocument> findVerticesByLabel(
            @PathVariable String datasource,
            @RequestParam(value = "q") String label
    ) throws Exception {
        return base.findVerticesByDatasourceAndLabel(datasource, label);
    }

    @GetMapping(value = "/{datasource}/e/label")
    public List<ElasticEdgeDocument> findEdgesByLabel(
            @PathVariable String datasource,
            @RequestParam(value = "q") String label
    ) throws Exception {
        return base.findEdgesByDatasourceAndLabel(datasource, label);
    }

}
