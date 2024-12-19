package com.milvus.io.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;

public class MilvusSinkConnectorConfig extends AbstractConfig {
    protected static final String URL = "public.endpoint";
    protected static final String TOKEN = "token";
    protected static final String DATABASE_NAME = "database.name";
    protected static final String COLLECTION_NAME = "collection.name";

    public MilvusSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public MilvusSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(URL, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Public Endpoint")
                .define(TOKEN, ConfigDef.Type.PASSWORD, "db_admin:****", ConfigDef.Importance.HIGH, "Token to connect milvus")
                .define(DATABASE_NAME, ConfigDef.Type.STRING, "default", ConfigDef.Importance.MEDIUM, "Database name to save the topic messages")
                .define(COLLECTION_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Collection name to save the topic messages");
    }

    public String getUrl() {
        return getString(URL);
    }

    public Password getToken() {
        return getPassword(TOKEN);
    }

    public String getDatabaseName() {
        return getString(DATABASE_NAME);
    }

    public String getCollectionName() {
        return getString(COLLECTION_NAME);
    }
}
