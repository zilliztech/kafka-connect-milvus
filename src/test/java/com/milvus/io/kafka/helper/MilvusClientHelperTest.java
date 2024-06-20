package com.milvus.io.kafka.helper;

import com.milvus.io.kafka.MilvusSinkConnectorConfig;
import io.milvus.client.MilvusServiceClient;
import io.milvus.v2.client.MilvusClientV2;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MilvusClientHelperTest {
    @Mock
    MilvusClientHelper milvusClientHelper;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(milvusClientHelper.createMilvusClient(any(MilvusSinkConnectorConfig.class))).thenReturn(mock(MilvusClientV2.class));
    }

    @Test
    public void createMilvusClient() {

        milvusClientHelper.createMilvusClient(mock(MilvusSinkConnectorConfig.class));
        verify(milvusClientHelper, times(1)).createMilvusClient(any(MilvusSinkConnectorConfig.class));
    }
}