package com.milvus.io.kafka.helper;

import com.milvus.io.kafka.MilvusSinkConnectorConfig;
import com.milvus.io.kafka.utils.Utils;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;

public class MilvusClientHelper {
    public MilvusClientV2 createMilvusClient(MilvusSinkConnectorConfig config) {
        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri(config.getUrl())
                .token(Utils.decryptToken(config.getToken().value()))
                .build();
        return new MilvusClientV2(connectConfig);
    }
}
