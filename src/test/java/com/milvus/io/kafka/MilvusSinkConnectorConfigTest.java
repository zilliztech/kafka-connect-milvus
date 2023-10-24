package com.milvus.io.kafka;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class MilvusSinkConnectorConfigTest {

    @Test
    public void testConfig() {
        Map<String, String> props = new HashMap<>();
        props.put(MilvusSinkConnectorConfig.URL, "http://localhost:19121");
        props.put(MilvusSinkConnectorConfig.TOKEN, "token");
        props.put(MilvusSinkConnectorConfig.COLLECTION_NAME, "test_collection");
        MilvusSinkConnectorConfig config = new MilvusSinkConnectorConfig(props);
        Assert.assertEquals(config.getUrl(), props.get(MilvusSinkConnectorConfig.URL));
        Assert.assertEquals(config.getToken().value(), props.get(MilvusSinkConnectorConfig.TOKEN));
        Assert.assertEquals(config.getCollectionName(), props.get(MilvusSinkConnectorConfig.COLLECTION_NAME));
    }
}