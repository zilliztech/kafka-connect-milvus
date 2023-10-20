package com.milvus.io.kafka.utils;

import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldSchema;
import io.milvus.param.dml.InsertParam;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class DataConverterTest {
    CollectionSchema collectionSchema;

    DataConverter dataConverter;

    @Before
    public void setUp() {
        this.dataConverter = new DataConverter(null);

        //generate a Collection Schema like this for milvus collection
        //                .field("id", Schema.INT64_SCHEMA)
        //                .field("name", Schema.STRING_SCHEMA)
        //                .field("description", Schema.STRING_SCHEMA)
        //                .field("description_vector", Schema.ARRAY(Schema.FLOAT32_SCHEMA)
        //                .field("price", Schema.FLOAT32_SCHEMA)

        this.collectionSchema = CollectionSchema.newBuilder()
                .addFields(FieldSchema.newBuilder().setName("id").setDataType(DataType.Int64).build())
                .addFields(FieldSchema.newBuilder().setName("name").setDataType(DataType.VarChar).build())
                .addFields(FieldSchema.newBuilder().setName("description").setDataType(DataType.VarChar).build())
                .addFields(FieldSchema.newBuilder().setName("description_vector").setDataType(DataType.FloatVector).build())
                .addFields(FieldSchema.newBuilder().setName("price").setDataType(DataType.Float).build())
                .build();
    }

    @Test
    public void testConvertRecordWithMapValue() {
        HashMap<String, Object> value = new HashMap<>();
        value.put("id", 0L);
        value.put("name", "0");
        value.put("description", "0");
        value.put("description_vector", generateVector(8));
        value.put("price", 0F);
        SinkRecord sr = new SinkRecord("test", 0, null, null, null, value, 0);

        List<InsertParam.Field> fields = dataConverter.convertRecord(sr, collectionSchema);
        System.out.println(fields);
    }

    @Test
    public void testConvertRecordWithStructValue(){

        Schema schema = SchemaBuilder.struct().name("com.milvus.io.kafka.MilvusSinkTaskTest").version(1)
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("description", Schema.STRING_SCHEMA)
                .field("description_vector", SchemaBuilder.array(Schema.FLOAT32_SCHEMA).build())
                .field("price", Schema.FLOAT32_SCHEMA)
                .build();
        Struct value = new Struct(schema);
        value.put("id", 17694L);
        value.put("name", "17694");
        value.put("description", "17694");
        value.put("description_vector", generateVector(8));
        value.put("price", 17694F);

        SinkRecord sr = new SinkRecord("test", 0, null, null, schema, value, 0);

        List<InsertParam.Field> fields = dataConverter.convertRecord(sr, collectionSchema);
        System.out.println(fields);
    }

    public List<Float> generateVector(Integer dimensions){
        List<Float> floatList = new ArrayList<>();
        for (int k = 0; k < dimensions; ++k) {
            floatList.add(new Random().nextFloat());
        }
        return floatList;
    }
}