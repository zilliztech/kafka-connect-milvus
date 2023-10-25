package com.milvus.io.kafka.helper;

import com.milvus.io.kafka.MilvusSinkConnectorConfig;
import com.milvus.io.kafka.utils.Utils;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;

public class MilvusClientHelper {
    public MilvusServiceClient createMilvusClient(MilvusSinkConnectorConfig config) {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withUri(config.getUrl())
                .withToken(Utils.decryptToken(config.getToken().value()))
                .build();
        return new MilvusServiceClient(connectParam);
    }
}
