package com.milvus.io.kafka.client.request;

import com.google.gson.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
public class UpsertReq {
    private String dbName;
    private String collectionName;
    private String partitionName;
    private List<JsonObject> data;
}
