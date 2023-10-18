package com.milvus.io.kafka.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class VersionUtil {
    private static final String VERSION;

    static {
        Properties prop = new Properties();
        try (InputStream in = VersionUtil.class.getResourceAsStream("/kafka-connect-milvus.properties")) {
            prop.load(in);
            VERSION = prop.getProperty("version", "0.0.0.0");
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static String getVersion() {
        return VERSION;
    }
}
