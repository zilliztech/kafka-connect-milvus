package com.milvus.io.kafka.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.milvus.io.kafka.MilvusSinkConnectorConfig;
import io.milvus.param.dml.InsertParam;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class DataConverter {

    private final MilvusSinkConnectorConfig config;

    private static final Logger log = LoggerFactory.getLogger(DataConverter.class);

    public DataConverter(MilvusSinkConnectorConfig config) {
        this.config = config;
    }
    /*
        * Convert SinkRecord to JSONObject
     */
    public JSONObject convertRecord(SinkRecord sr, CreateCollectionReq.CollectionSchema collectionSchema) {
        // parse sinkRecord to get filed name and value
        if(sr.value() instanceof Struct) {
            return parseValue((Struct)sr.value(), collectionSchema);
        }else if (sr.value() instanceof HashMap) {
            return parseValue((HashMap<?, ?>)sr.value(), collectionSchema);
        }else {
            throw new RuntimeException("Unsupported SinkRecord data type" + sr.value());
        }
    }

    private JSONObject parseValue(HashMap<?, ?> mapValue, CreateCollectionReq.CollectionSchema collectionSchema) {
        JSONObject fields = new JSONObject();
        mapValue.forEach((field, value) -> {
            if(collectionSchema.getField(field.toString())!=null){
                // if the key exists in the collection, store the value by collectionSchema DataType
                fields.put(field.toString(), castValueToType(value, collectionSchema.getField(field.toString()).getDataType()));
            }else {
                log.warn("Field {} not exists in collection", field);
            }

        });
        return fields;
    }

    private JSONObject parseValue(Struct structValue, CreateCollectionReq.CollectionSchema collectionSchema) {
        JSONObject fields = new JSONObject();

        structValue.schema().fields().forEach(field -> {
            if(collectionSchema.getField(field.name()) != null){
                // if the key exists in the collection, store the value by collectionSchema DataType
                fields.put(field.toString(), castValueToType(structValue.get(field.name()), collectionSchema.getField(field.name()).getDataType()));
            }else {
                log.warn("Field {} not exists in collection", field);
            }
        });

        return fields;
    }

    private Object castValueToType(Object value, DataType dataType) {
        switch (dataType){
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
                Gson gson = new Gson();
                return gson.toJson(value);
            case BinaryVector:
                return parseBinaryVectorField(value.toString());
            case FloatVector:
                return parseFloatVectorField(value.toString());
            default:
                throw new RuntimeException("Unsupported data type" + dataType);
        }
    }

    protected  List<Float> parseFloatVectorField(String vectors){
        try {
            log.debug("parse float vectors: {}", vectors);

            String[] vectorArrays = vectors.replaceAll("\\[", "").replaceAll("\\]", "")
                    .replaceAll(" ","").split(",");

            List<Float> floatList = Lists.newLinkedList();
            for (String vector : vectorArrays) {
                floatList.add(Float.valueOf(vector));
            }

            return floatList;
        }catch (Exception e){
            throw new RuntimeException("parse float vector field error: " + e.getMessage() + vectors);
        }

    }
    protected  ByteBuffer parseBinaryVectorField(String vectors){
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
        }catch (Exception e){
            throw new RuntimeException("parse binary vector field error: " + e.getMessage() + vectors);
        }
    }
}
