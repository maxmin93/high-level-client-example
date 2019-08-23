package com.example.aws.elasticsearch.demo.basegraph;

public interface BaseTx extends AutoCloseable {

    void failure();
    void success();
    void close();

}