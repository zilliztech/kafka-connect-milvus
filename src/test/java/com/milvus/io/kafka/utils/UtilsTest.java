package com.milvus.io.kafka.utils;

import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {

    @Test
    public void encryptToken() {
        String token = "test";
        String encryptedToken = Utils.encryptToken(token);
        String decryptedToken = Utils.decryptToken(encryptedToken);
        Assert.assertEquals(token, decryptedToken);
    }
}