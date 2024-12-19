package com.milvus.io.kafka.client.common;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import java.lang.reflect.Type;

public class JsonUtils {
    private static final Gson GSON_INSTANCE;

    static {
        GSON_INSTANCE = (new GsonBuilder()).serializeNulls().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();
    }

    public JsonUtils() {
    }

    public static <T> T fromJson(String jsonStr, Type typeOfT) {
        return GSON_INSTANCE.fromJson(jsonStr, typeOfT);
    }

    public static String toJson(Object object) {
        return GSON_INSTANCE.toJson(object);
    }
}
