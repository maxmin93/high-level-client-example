package com.example.aws.elasticsearch.demo.web;

import com.example.aws.elasticsearch.demo.profilesample.model.ProfileDocument;
import com.example.aws.elasticsearch.demo.profilesample.ProfileService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/profile")
public class ProfileController {

    private ProfileService service;

    @Autowired
    public ProfileController(
            ProfileService service
    ) {
        this.service = service;
    }

    @GetMapping("/hello")
    public String test(){
        return "Hello world";
    }

    ///////////////////////////////////////////////////////////////

    // curl -X GET "localhost:8080/index"
    @GetMapping("/index")
    public ResponseEntity checkExistsIndex() throws Exception {
        return new ResponseEntity(service.checkExistsIndex(), HttpStatus.OK);
    }

    @PostMapping("/index")
    public ResponseEntity createIndex() throws Exception {
        return new ResponseEntity(service.createIndex(), HttpStatus.OK);
    }

    @DeleteMapping("/index")
    public ResponseEntity removeIndex() throws Exception {
        return new ResponseEntity(service.removeIndex(), HttpStatus.OK);
    }

    @PutMapping("/index")
    public ResponseEntity resetIndex() throws Exception {
        return new ResponseEntity(service.resetIndex(), HttpStatus.OK);
    }

    ///////////////////////////////////////////////////////////////

    // curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"profile_01", "first_name":"world1", "last_name": "hello", "gender":"male", "age":23, "emails": ["world1.hello@mail.com"], "technologies": [ {"name":"java", "years_of_experience":5},{"name":"typescript", "years_of_experience":3}] }' localhost:8080/doc
    // curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"profile_02", "first_name":"world2", "last_name": "hello", "gender":"female", "age":32, "emails": ["world2.hello@mail.com"], "technologies": [ {"name":"ruby", "years_of_experience":6},{"name":"jruvy", "years_of_experience":1}] }' localhost:8080/doc
    // curl -X POST -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"profile_03", "first_name":"world3", "last_name": "fake", "gender":"male", "age":47, "emails": ["world3.fake@junk.com"], "technologies": [ {"name":"samba", "years_of_experience":4},{"name":"hotdog", "years_of_experience":2}] }' localhost:8080/doc
    @PostMapping("/doc")
    public ResponseEntity createProfile(@RequestBody ProfileDocument document) throws Exception {
        return new ResponseEntity(service.createProfileDocument(document), HttpStatus.CREATED);
    }

    // curl -X PUT -H "Content-Type: application/json; charset=utf-8" -d '{ "id":"profile_02", "first_name":"world2", "last_name": "hello", "gender":"female", "age":31, "emails": ["world2.hello@mail.com", "world2.new@another.com"], "technologies": [ {"name":"javascript", "years_of_experience":2}, {"name":"ruby", "years_of_experience":3},{"name":"jruby", "years_of_experience":1}] }' localhost:8080/doc
    @PutMapping("/doc")
    public ResponseEntity updateProfile(@RequestBody ProfileDocument document) throws Exception {
        return new ResponseEntity(service.updateProfileDocument(document), HttpStatus.CREATED);
    }

    // curl -X DELETE "localhost:8080/doc/profile_03"
    @DeleteMapping("/doc/{id}")
    public String deleteProfileDocument(@PathVariable String id) throws Exception {

        return service.deleteProfileDocument(id);
    }

    ///////////////////////////////////////////////////////////////

    // curl -X GET "localhost:8080/doc"
    @GetMapping("/doc")
    public List<ProfileDocument> findAll() throws Exception {

        return service.findAll();
    }

    // curl -X GET "localhost:8080/doc/profile_02"
    @GetMapping("/doc/{id}")
    public ProfileDocument findById(@PathVariable String id) throws Exception {

        return service.findById(id);
    }

    ///////////////////////////////////////////////////////////////

    // curl -X GET "localhost:8080/doc/search/technology?q=java"
    @GetMapping(value = "/doc/search/technology")
    public List<ProfileDocument> searchByTechnology(
            @RequestParam(value = "q") String technology
    ) throws Exception {

        return service.searchByTechnology(technology);
    }

    // curl -X GET "localhost:8080/doc/search/name?q=hello"
    @GetMapping(value = "/doc/search/name")
    public List<ProfileDocument> searchByName(
            @RequestParam(value = "q") String name
    ) throws Exception {

        return service.searchByName(name);
    }

}
