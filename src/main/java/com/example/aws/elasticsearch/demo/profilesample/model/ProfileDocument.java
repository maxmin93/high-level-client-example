package com.example.aws.elasticsearch.demo.profilesample.model;

import lombok.Data;

import java.util.List;

@Data
public class ProfileDocument {

    private String id;
    private String first_name;
    private String last_name;
    private String gender;
    private Integer age;
    private List<String> emails;
    private List<Technologies> technologies;

}
