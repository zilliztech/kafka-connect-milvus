package com.milvus.io.kafka;

import io.milvus.client.MilvusClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.*;

public class MilvusSinkTaskTest {

    @Mock
    MilvusSinkTask task;
    @Mock
    MilvusClient myMilvusClient;

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void version() {
        MilvusSinkTask task = new MilvusSinkTask();
        System.out.println(task.version());
    }
}