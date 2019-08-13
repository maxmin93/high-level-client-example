package com.example.aws.elasticsearch.demo.document;

import com.example.aws.elasticsearch.demo.model.Technologies;
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
