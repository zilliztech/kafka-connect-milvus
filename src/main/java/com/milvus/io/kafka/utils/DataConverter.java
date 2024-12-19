package com.milvus.io.kafka.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.milvus.io.kafka.MilvusSinkConnectorConfig;
import com.milvus.io.kafka.client.common.DataType;
import com.milvus.io.kafka.client.response.DescribeCollectionResp;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DataConverter {

    private static final Logger log = LoggerFactory.getLogger(DataConverter.class);
    private final MilvusSinkConnectorConfig config;

    public DataConverter(MilvusSinkConnectorConfig config) {
        this.config = config;
    }

    /*
     * Convert SinkRecord to JsonObject
     */
    public JsonObject convertRecord(SinkRecord sr, DescribeCollectionResp describeCollectionResp) {
        // parse sinkRecord to get field name and value
        if (sr.value() instanceof Struct) {
            return parseValue((Struct) sr.value(), describeCollectionResp);
        } else if (sr.value() instanceof HashMap) {
            return parseValue((HashMap<?, ?>) sr.value(), describeCollectionResp);
        } else {
            throw new RuntimeException("Unsupported SinkRecord data type: " + sr.value());
        }
    }

    private JsonObject parseValue(HashMap<?, ?> mapValue, DescribeCollectionResp collectionSchema) {
        JsonObject fields = new JsonObject();
        Gson gson = new Gson();
        mapValue.forEach((field, value) -> {
            if (collectionSchema.getField(field.toString()) != null) {
                // if the key exists in the collection, store the value by collectionSchema DataType
                Object object = convertValueByMilvusType(value, collectionSchema.getField(field.toString()).getType());
                fields.add(field.toString(), gson.toJsonTree(object));
            } else {
                log.warn("Field {} not exists in collection", field);
            }
        });
        return fields;
    }

    private JsonObject parseValue(Struct structValue, DescribeCollectionResp collectionSchema) {
        JsonObject fields = new JsonObject();
        Gson gson = new Gson();
        structValue.schema().fields().forEach(field -> {
            if (collectionSchema.getField(field.name()) != null) {
                // if the key exists in the collection, store the value by collectionSchema DataType
                Object object = convertValueByMilvusType(structValue.get(field.name()), collectionSchema.getField(field.name()).getType());
                fields.add(field.name(), gson.toJsonTree(object));
            } else {
                log.warn("Field {} not exists in collection", field);
            }
        });

        return fields;
    }

    private Object convertValueByMilvusType(Object value, String dataType) {
        DataType type = DataType.valueOf(dataType);
        Gson gson = new Gson();
        switch (type) {
            case Bool:
                return Boolean.parseBoolean(value.toString());
            case Int8:
            case Int16:
                return Short.parseShort(value.toString());
            case Int32:
                return Integer.parseInt(value.toString());
            case Int64:
                return Long.parseLong(value.toString());
            case Float:
                return Float.parseFloat(value.toString());
            case Double:
                return Double.parseDouble(value.toString());
            case VarChar:
            case String:
                return value.toString();
            case JSON:
                return gson.toJson(value);
            case BinaryVector:
                return parseBinaryVectorField(value.toString());
            case FloatVector:
                return parseFloatVectorField(value.toString());
            case SparseFloatVector:
                return gson.toJsonTree(value).getAsJsonObject();
            default:
                throw new RuntimeException("Unsupported data type: " + dataType);
        }
    }

    protected List<Float> parseFloatVectorField(String vectors) {
        try {
            log.debug("parse float vectors: {}", vectors);

            String[] vectorArrays = vectors.replaceAll("\\[", "").replaceAll("\\]", "")
                    .replaceAll(" ", "").split(",");

            List<Float> floatList = new ArrayList<>();
            for (String vector : vectorArrays) {
                floatList.add(Float.valueOf(vector));
            }

            return floatList;
        } catch (Exception e) {
            throw new RuntimeException("parse float vector field error: " + e.getMessage() + " " + vectors);
        }
    }

    protected ByteBuffer parseBinaryVectorField(String vectors) {
        try {
            log.debug("parse binary vectors: {}", vectors);

            String[] vectorArrays = vectors.replaceAll("\\[", "").replaceAll("\\]", "")
                    .replaceAll(" ", "").split(",");

            ByteBuffer buffer = ByteBuffer.allocate(vectorArrays.length);
            for (String vectorArray : vectorArrays) {
                int vector = Integer.parseInt(vectorArray);
                buffer.put((byte) vector);
            }

            return buffer;
        } catch (Exception e) {
            throw new RuntimeException("parse binary vector field error: " + e.getMessage() + " " + vectors);
        }
    }
}