package com.milvus.io.kafka.helper;

import com.milvus.io.kafka.MilvusSinkConnectorConfig;
import com.milvus.io.kafka.client.MilvusRestClient;
import com.milvus.io.kafka.utils.Utils;

public class MilvusClientHelper {
    public MilvusRestClient createMilvusClient(MilvusSinkConnectorConfig config) {
        return new MilvusRestClient(config.getUrl(), Utils.decryptToken(config.getToken().value()), config.getDatabaseName());
    }
}
