package com.milvus.io.kafka.utils;

import com.alibaba.fastjson.JSONObject;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldSchema;
import io.milvus.param.dml.InsertParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class DataConverterTest {
    CreateCollectionReq.CollectionSchema collectionSchema;

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
        this.collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder().fieldName("id").dataType(io.milvus.v2.common.DataType.Int64).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("name").dataType(io.milvus.v2.common.DataType.VarChar).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("description").dataType(io.milvus.v2.common.DataType.VarChar).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("description_vector").dataType(io.milvus.v2.common.DataType.FloatVector).dimension(2).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("price").dataType(io.milvus.v2.common.DataType.Float).build());

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

        JSONObject expected_field = new JSONObject();
        expected_field.putAll(value);

        JSONObject fields = dataConverter.convertRecord(sr, collectionSchema);

        // assert if expected_fields have the same size and same elements with fields
        Assert.assertEquals(expected_field, fields);
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

        JSONObject fields = dataConverter.convertRecord(sr, collectionSchema);

        Assert.assertEquals(5, fields.size());
        fields.forEach((k, v) -> {
            switch (k) {
                case "id":
                    Assert.assertEquals(17694L, v);
                    break;
                case "name":
                    Assert.assertEquals("17694", v);
                    break;
                case "description":
                    Assert.assertEquals("17694", v);
                    break;
                case "description_vector":
                    Assert.assertEquals(8, ((List<Float>) v).size());
                    break;
                case "price":
                    Assert.assertEquals(17694F, v);
                    break;
            }
        });
    }

    public List<Float> generateVector(Integer dimensions){
        List<Float> floatList = new ArrayList<>();
        for (int k = 0; k < dimensions; ++k) {
            floatList.add(new Random().nextFloat());
        }
        return floatList;
    }
}