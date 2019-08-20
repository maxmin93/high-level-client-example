package com.example.aws.elasticsearch.demo.elasticgraph.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.ResourceUtils;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;

import static com.example.aws.elasticsearch.demo.profilesample.Constant.INDEX;

public class ElasticGraphService {

    static final String MAPPINGS_VERTEX = "classpath:mappings/vertex-document.json";
    static final String MAPPINGS_EDGE = "classpath:mappings/edge-document.json";

    public static final String INDEX_VERTEX = "elasticvertex";
    public static final String INDEX_EDGE = "elasticedge";

    private RestHighLevelClient client;
    private ObjectMapper objectMapper;

    @Autowired
    public ElasticGraphService(
            RestHighLevelClient client,     // elasticsearch config
            ObjectMapper objectMapper       // spring boot web starter
    ) {
        this.client = client;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    private void ready() throws Exception {
        // if not exists index, create index
        if( !checkExistsIndex(INDEX_VERTEX) ) createIndex(INDEX_VERTEX);
        if( !checkExistsIndex(INDEX_EDGE) ) createIndex(INDEX_EDGE);
    }

    ///////////////////////////////////////////////////////////////

    // check if exists index
    private boolean checkExistsIndex(String index) throws  Exception {
        GetIndexRequest request = new GetIndexRequest(index);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    private String readMappings(String index) throws Exception {
        String mappings_file = index.equals(INDEX_VERTEX) ? MAPPINGS_VERTEX : MAPPINGS_EDGE;
        File file = ResourceUtils.getFile(mappings_file);
        if( !file.exists() ) throw new FileNotFoundException("mappings not found => "+mappings_file);
        return new String(Files.readAllBytes(file.toPath()));
    }

    private boolean createIndex(String index) throws Exception {

        CreateIndexRequest request = new CreateIndexRequest(index);

        // settings
        request.settings(Settings.builder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 0)
        );
        // mappings
        request.mapping(readMappings(index), XContentType.JSON);

        AcknowledgedResponse indexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        return indexResponse.isAcknowledged();
    }

    private boolean removeIndex(String index) throws Exception {
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        AcknowledgedResponse indexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
        return indexResponse.isAcknowledged();
    }

    public boolean resetIndex() throws Exception {
        boolean result = true;

        if( checkExistsIndex(INDEX_VERTEX) ) removeIndex(INDEX_VERTEX);
        result &= createIndex(INDEX_VERTEX);
        if( checkExistsIndex(INDEX_EDGE) ) removeIndex(INDEX_EDGE);
        result &= createIndex(INDEX_EDGE);

        return result;
    }

}
