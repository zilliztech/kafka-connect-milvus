package com.milvus.io.kafka.utils;

import org.junit.Assert;
import org.junit.Test;

public class VersionUtilTest {

    @Test
    public void testGetVersion() {
        String version = VersionUtil.getVersion();
        System.out.println(version);
        Assert.assertNotNull(version);
        Assert.assertNotEquals("0.0.0.0", version);
    }
}