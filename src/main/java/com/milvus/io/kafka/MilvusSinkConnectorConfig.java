package com.milvus.io.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MilvusSinkConnectorConfig extends AbstractConfig {
    public static final String URL = "public.endpoint";
    public static final String TOKEN = "token";
    private static final String COLLECTION_NAME = "collection.name";

    public MilvusSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public MilvusSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(URL, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Public Endpoint")
                .define(TOKEN, ConfigDef.Type.STRING, "db_admin:****", ConfigDef.Importance.HIGH, "Token to connect milvus")
                .define(COLLECTION_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Collection name to save the topic messages");
    }

    public String getUrl() {
        return getString(URL);
    }

    public String getToken() {
        return getString(TOKEN);
    }

    public String getCollectionName(){return getString(COLLECTION_NAME);}
}
