package com.milvus.io.kafka.client;

import com.google.gson.reflect.TypeToken;
import com.milvus.io.kafka.client.common.JsonUtils;
import com.milvus.io.kafka.client.request.UpsertReq;
import com.milvus.io.kafka.client.response.DescribeCollectionResp;
import com.milvus.io.kafka.client.response.GetLoadStateResp;
import com.milvus.io.kafka.client.response.HasCollectionResp;
import com.milvus.io.kafka.client.response.RestfulResponse;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MilvusRestClient {
    private static final Logger log = LoggerFactory.getLogger(MilvusRestClient.class);

    private final String url;
    private final String token;
    private final String database;

    public MilvusRestClient(String url, String token, String database) {
        this.url = url;
        this.token = token;
        this.database = database;
    }

    public Boolean hasCollection(String collectionName) {
        String endpoint = url + "/v2/vectordb/collections/has";
        Map<String, Object> params = createBaseParams(collectionName);

        String response = doPost(endpoint, params);
        HasCollectionResp restfulResponse = JsonUtils.fromJson(response, new TypeToken<HasCollectionResp>() {}.getType());
        return restfulResponse.getHas();
    }

    public DescribeCollectionResp describeCollection(String collectionName) {
        String endpoint = url + "/v2/vectordb/collections/describe";
        Map<String, Object> params = createBaseParams(collectionName);

        String response = doPost(endpoint, params);
        return JsonUtils.fromJson(response, new TypeToken<DescribeCollectionResp>() {}.getType());
    }

    public GetLoadStateResp getLoadState(String collectionName) {
        String endpoint = url + "/v2/vectordb/collections/get_load_state";
        Map<String, Object> params = createBaseParams(collectionName);

        String response = doPost(endpoint, params);
        return JsonUtils.fromJson(response, new TypeToken<GetLoadStateResp>() {}.getType());
    }

    public void upsert(UpsertReq upsertReq) {
        upsertReq.setDbName(database);
        String endpoint = url + "/v2/vectordb/entities/upsert";
        doPost(endpoint, upsertReq);
    }

    private String doPost(String endpoint, Object params) {
        try {
            HttpResponse<String> response = Unirest.post(endpoint)
                    .header("Authorization", "Bearer " + token)
                    .header("Content-Type", "application/json")
                    .body(JsonUtils.toJson(params))
                    .asString();

            if (response.getStatus() != 200) {
                log.error("HTTP Error {}: {}", response.getStatus(), response.getStatusText());
                throw new RuntimeException("Failed to call Milvus server");
            }

            RestfulResponse<Object> restfulResponse = JsonUtils.fromJson(response.getBody(), new TypeToken<RestfulResponse<Object>>() {}.getType());

            if (restfulResponse.getCode() != 0) {
                log.error("Milvus API Error: {}", restfulResponse.getMessage());
                throw new RuntimeException("Milvus server returned an error: " + restfulResponse.getMessage());
            }

            return JsonUtils.toJson(restfulResponse.getData());
        } catch (Exception e) {
            log.error("Error calling Milvus server at {}: {}", endpoint, e.getMessage());
            throw new RuntimeException("Failed to call Milvus server", e);
        }
    }

    private Map<String, Object> createBaseParams(String collectionName) {
        Map<String, Object> params = new HashMap<>();
        params.put("dbName", database);
        params.put("collectionName", collectionName);
        return params;
    }
}