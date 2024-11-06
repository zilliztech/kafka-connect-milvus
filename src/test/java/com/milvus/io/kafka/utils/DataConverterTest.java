package com.milvus.io.kafka.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class DataConverterTest {
    CreateCollectionReq.CollectionSchema collectionSchema;

    DataConverter dataConverter;

    MilvusClientV2 client;

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
        collectionSchema.addField(AddFieldReq.builder().fieldName("id").dataType(io.milvus.v2.common.DataType.Int64).isPrimaryKey(true).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("name").dataType(io.milvus.v2.common.DataType.VarChar).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("description").dataType(io.milvus.v2.common.DataType.VarChar).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("description_vector").dataType(io.milvus.v2.common.DataType.FloatVector).dimension(8).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("sparse_vector").dataType(DataType.SparseFloatVector).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("price").dataType(io.milvus.v2.common.DataType.Float).build());

//        ConnectConfig config = ConnectConfig.builder()
//                .uri("")
//                .token("")
//                .build();
//        client = new MilvusClientV2(config);
//        if(client.hasCollection(HasCollectionReq.builder().collectionName("test").build())) {
//            client.dropCollection(DropCollectionReq.builder().collectionName("test").build());
//        }
//        client.createCollection(CreateCollectionReq.builder()
//                .collectionName("test")
//                .collectionSchema(collectionSchema)
//                .build());

    }
    @After
    public void tearDown() {
        //client.dropCollection(DropCollectionReq.builder().collectionName("test").build());
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

        Gson gson = new Gson();
        JsonObject expected_field = gson.toJsonTree(value).getAsJsonObject();


        JsonObject fields = dataConverter.convertRecord(sr, collectionSchema);

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
                .field("sparse_vector", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT32_SCHEMA).build())
                .field("price", Schema.FLOAT32_SCHEMA)
                .build();
        Struct value = new Struct(schema);
        value.put("id", 17694L);
        value.put("name", "17694");
        value.put("description", "17694");
        value.put("description_vector", generateVector(8));
        HashMap<String, Float> sparseVector = new HashMap<>();
        sparseVector.put("1", 0.1F);
        value.put("sparse_vector", sparseVector);
        value.put("price", 17694F);


        SinkRecord sr = new SinkRecord("test", 0, null, null, schema, value, 0);

        JsonObject fields = dataConverter.convertRecord(sr, collectionSchema);
        Assert.assertEquals(6, fields.size());

        fields.entrySet().forEach(entry -> {
            switch (entry.getKey()) {
                case "id":
                    Assert.assertEquals(17694L, entry.getValue().getAsLong());
                    break;
                case "name":
                    Assert.assertEquals("17694", entry.getValue().getAsString());
                    break;
                case "description":
                    Assert.assertEquals("17694", entry.getValue().getAsString());
                    break;
                case "description_vector":
                    Assert.assertEquals(8, entry.getValue().getAsJsonArray().size());
                    break;
                case "price":
                    Assert.assertEquals(17694L, entry.getValue().getAsLong());
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