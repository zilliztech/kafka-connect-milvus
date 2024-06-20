package com.milvus.io.kafka;

import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.GetLoadStateResponse;
import io.milvus.grpc.LoadState;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.InsertReq;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static com.milvus.io.kafka.MilvusSinkConnectorConfig.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MilvusSinkTaskTest {

    @InjectMocks
    private MilvusSinkTask task;
    @Mock
    private MilvusClientV2 milvusClient;

    private Map<String, String> props;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        props = new HashMap<>();
        props.put(URL, "https://in01-aaaaaaaaa.aws-us-west-2.vectordb.zillizcloud.com:19541");
        props.put(TOKEN, "token_test");
        props.put(COLLECTION_NAME, "collection_test");
        // Add other required properties

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Int64).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("name").dataType(DataType.VarChar).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("description").dataType(DataType.VarChar).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("description_vector").dataType(DataType.FloatVector).dimension(2).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("price").dataType(DataType.Float).build());

        DescribeCollectionResp mockDescribeR = DescribeCollectionResp.builder()
                .collectionSchema(collectionSchema)
                .autoID(true)
                .build();
        when(milvusClient.hasCollection(any())).thenReturn(true);
        when(milvusClient.describeCollection(any(DescribeCollectionReq.class))).thenReturn(mockDescribeR);
        when(milvusClient.getLoadState(any(GetLoadStateReq.class))).thenReturn(true);

    }

    @Test
    public void testMilvusSinkTaskLifecycle() {

        task.start(props, milvusClient);

        HashMap<String, Object> value = new HashMap<>();
        value.put("id", 0L);
        value.put("name", "0");
        value.put("description", "0");
        value.put("description_vector", generateVector(8));
        value.put("price", 0F);
        SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);

        task.put(Collections.singleton(record));

        task.stop();

        // Verify that the client methods were called with the expected parameters
        verify(milvusClient).describeCollection(any(DescribeCollectionReq.class));
        verify(milvusClient).getLoadState(any(GetLoadStateReq.class));
        verify(milvusClient).insert(any(InsertReq.class));
    }

    public List<Float> generateVector(Integer dimensions){
        List<Float> floatList = new ArrayList<>();
        for (int k = 0; k < dimensions; ++k) {
            floatList.add(new Random().nextFloat());
        }
        return floatList;
    }
}
