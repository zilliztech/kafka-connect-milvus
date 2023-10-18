package com.milvus.io.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MilvusSinkConnectorConfig extends AbstractConfig {
    public static final String URL = "public.endpoint";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
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
                .define(USERNAME, ConfigDef.Type.STRING, "db_admin", ConfigDef.Importance.MEDIUM, "Username")
                .define(PASSWORD, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Password")
                .define(COLLECTION_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Collection name to save the topic messages");
    }

    public String getUrl() {
        return getString(URL);
    }

    public String getUsername() {
        return getString(USERNAME);
    }

    public String getPassword() {
        return getString(PASSWORD);
    }

    public String getCollectionName(){return getString(COLLECTION_NAME);}
}
