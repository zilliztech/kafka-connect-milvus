package com.milvus.io.kafka.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.milvus.io.kafka.MilvusSinkConnectorConfig;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldSchema;
import io.milvus.param.dml.InsertParam;
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
        * Convert SinkRecord to List<InsertParam.Field>
     */
    public List<InsertParam.Field> convertRecord(SinkRecord sr, CollectionSchema collectionSchema) {
        // parse sinkRecord to get filed name and value
        if(sr.value() instanceof Struct) {
            return parseValue((Struct)sr.value(), collectionSchema);
        }else if (sr.value() instanceof HashMap) {
            return parseValue((HashMap<?, ?>)sr.value(), collectionSchema);
        }else {
            throw new RuntimeException("Unsupported SinkRecord data type" + sr.value());
        }
    }

    private List<InsertParam.Field> parseValue(HashMap<?, ?> mapValue, CollectionSchema collectionSchema) {
        List<InsertParam.Field> fields = new ArrayList<>();
        // convert collectionSchema.getFieldsList: Filed's Name and DataType to a Map
        Map<String, DataType> fieldType = collectionSchema.getFieldsList().stream().collect(Collectors.toMap(FieldSchema::getName, FieldSchema::getDataType));
        mapValue.forEach((key1, value) -> {
            // for each field, create a InsertParam.Field
            if(fieldType.containsKey(key1.toString())){
                // if the key exists in the collection, store the value by collectionSchema DataType
                fields.add(new InsertParam.Field(key1.toString(), Collections.singletonList(castValueToType(value, fieldType.get(key1.toString())))));
            }else if(collectionSchema.getEnableDynamicField()){
                // if the key not exists in the collection and the collection is dynamic, store the value directly
                fields.add(new InsertParam.Field(key1.toString(), Collections.singletonList(value)));
            }
        });
        return fields;
    }

    private List<InsertParam.Field> parseValue(Struct structValue, CollectionSchema collectionSchema) {
        List<InsertParam.Field> fields = new ArrayList<>();
        // convert collectionSchema.getFieldsList: Filed's Name and DataType to a Map
        Map<String, DataType> fieldType = collectionSchema.getFieldsList().stream().collect(Collectors.toMap(FieldSchema::getName, FieldSchema::getDataType));
        structValue.schema().fields().forEach(field -> {
            // for each field, create a InsertParam.Field
            if(fieldType.containsKey(field.name())){
                // if the key exists in the collection, store the value by collectionSchema DataType
                fields.add(new InsertParam.Field(field.name(), Collections.singletonList(castValueToType(structValue.get(field.name()), fieldType.get(field.name())))));
            }else if(collectionSchema.getEnableDynamicField()){
                // if the key not exists in the collection and the collection is dynamic, store the value directly
                fields.add(new InsertParam.Field(field.name(), Collections.singletonList(structValue.get(field.name()))));
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

    public List<JSONObject> convertRecordWithDynamicSchema(SinkRecord sr, CollectionSchema collectionSchema) {
        List<InsertParam.Field> fields = convertRecord(sr, collectionSchema);
        List<JSONObject> jsonObjects = new ArrayList<>();
        int rows = fields.get(0).getValues().size();
        for (int i = 0; i < rows; i++) {
            JSONObject jsonObject = new JSONObject();
            for (InsertParam.Field field : fields) {
                jsonObject.put(field.getName(), field.getValues().get(i));
            }
            jsonObjects.add(jsonObject);
        }
        return jsonObjects;
    }
}
