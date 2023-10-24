package com.milvus.io.kafka.utils;

import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldSchema;
import io.milvus.param.dml.InsertParam;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

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
        List<InsertParam.Field> expected_fields = new ArrayList<>();
        value.forEach((key1, value1) -> {
            expected_fields.add(new InsertParam.Field(key1, Collections.singletonList(value1)));
        });

        List<InsertParam.Field> fields = dataConverter.convertRecord(sr, collectionSchema);

        // assert if expected_fields have the same size and same elements with fields
        Assert.assertEquals(expected_fields.size(), fields.size());

        for (InsertParam.Field expectedField : expected_fields) {
            boolean found = false;
            for (InsertParam.Field field : fields) {
                if (expectedField.getName().equals(field.getName()) &&
                        expectedField.getValues().equals(field.getValues())) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue(found);
        }

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

        Assert.assertEquals(5, fields.size());
        fields.forEach(field -> {
            Assert.assertEquals(value.get(field.getName()), field.getValues().get(0));
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