package com.milvus.io.kafka.client.response;

import com.google.gson.annotations.SerializedName;
import com.milvus.io.kafka.client.common.ConsistencyLevel;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Data
@NoArgsConstructor
public class DescribeCollectionResp {

    @SerializedName("collectionName")
    private String collectionName;

    @SerializedName("description")
    private String description = "";

    @SerializedName("autoId")
    private Boolean autoID;

    @SerializedName("enableDynamicField")
    private Boolean enableDynamicField;

    @SerializedName("consistencyLevel")
    private ConsistencyLevel consistencyLevel;

    @SerializedName("collectionID")
    private Long collectionID;

    @SerializedName("fields")
    private List<FieldSchema> fields;

    @SerializedName("indexes")
    private List<IndexSchema> indexes;

    @SerializedName("load")
    private String loadState;

    @SerializedName("partitionsNum")
    private Integer partitionsNum;

    @SerializedName("shardsNum")
    private Integer shardsNum;

    @SerializedName("properties")
    private List<Map<String, String>> properties;

    public FieldSchema getField(String name) {
        for (FieldSchema field : fields) {
            if (Objects.equals(field.name, name)) {
                return field;
            }
        }
        return null;
    }

    // Nested FieldSchema Class
    @Data
    @NoArgsConstructor
    public static class FieldSchema {
        @SerializedName("id")
        private Integer id;

        @SerializedName("name")
        private String name;

        @SerializedName("type")
        private String type;

        @SerializedName("description")
        private String description;

        @SerializedName("primaryKey")
        private Boolean primaryKey;

        @SerializedName("partitionKey")
        private Boolean partitionKey;

        @SerializedName("autoId")
        private Boolean autoId;

        @SerializedName("clusteringKey")
        private Boolean clusteringKey;

        @SerializedName("params")
        private List<Map<String, String>> params;
    }

    // Nested IndexSchema Class
    @Data
    @NoArgsConstructor
    public static class IndexSchema {
        @SerializedName("fieldName")
        private String fieldName;

        @SerializedName("indexName")
        private String indexName;

        @SerializedName("metricType")
        private String metricType;
    }
}