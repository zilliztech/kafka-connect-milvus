package com.milvus.io.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class MilvusSinkTaskTest {

    private MilvusSinkTask task;

    @Before
    public void setUp() {
        task = new MilvusSinkTask();
        Map<String, String> props = new HashMap<>();
        props.put("public.endpoint", "https://in01-034b8444ab99cab.aws-us-west-2.vectordb.zillizcloud.com:19532"); // You can add actual config key-value pairs here
        props.put("token", "db_admin:Zilliz@2023");
        props.put("collection.name", "sample_data_dynamic");
        task.start(props);
    }

    @After
    public void tearDown() {
        task.stop();
    }

    @Test
    public void testStart() {

    }

    @Test
    public void testPutMap(){
        HashMap<String, Object> value = new HashMap<>();
        value.put("id", 17694);
        value.put("name", "17694");
        value.put("description", generateVector(1));
        value.put("price", 17694);
        SinkRecord sr = new SinkRecord("test", 0, null, null, null, value, 0);
        task.put(Collections.singleton(sr));
    }

    @Test
    public void testPutStruct(){
        // generate schema
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("description", SchemaBuilder.array(Schema.FLOAT32_SCHEMA).build())
                .field("price", Schema.INT64_SCHEMA)
                .build();
        // generate struct
        Struct struct = new Struct(schema)
                .put("id", 17694L)
                .put("name", "17694")
                .put("description", generateVector(1))
                .put("price", 17694L);
        SinkRecord sr = new SinkRecord("test", 0, null, null, schema, struct, 0);
        task.put(Collections.singleton(sr));
    }

    @Test
    public void testPutWithDynamicSchema(){
        HashMap<String, Object> value = new HashMap<>();
        value.put("id", 17694);
        value.put("name", "17694");
        value.put("description", generateVector(1));
        value.put("price", 17694);
        value.put("dynamic", "dynamic");
        SinkRecord sr = new SinkRecord("test", 0, null, null, null, value, 0);
        task.put(Collections.singleton(sr));
    }

    @Test
    public void testJsonDeserialization(){
        String json = "{\"id\": 17694, \"name\": \"17694\", \"description\": \"17694\", \"price\": 17694}";
        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(Collections.singletonMap("schemas.enable", false), false);
        byte[] bytes = jsonConverter.fromConnectData("test", null, json);
        System.out.println(Arrays.toString(bytes));
        Object a = jsonConverter.toConnectData("test", bytes).value();
        System.out.println(a);
        jsonConverter.close();
    }

    public List<Float> generateVector(Integer dimensions){
        List<Float> floatList = new ArrayList<>();
        for (int k = 0; k < dimensions; ++k) {
            floatList.add(new Random().nextFloat());
        }
        return floatList;
    }
}
