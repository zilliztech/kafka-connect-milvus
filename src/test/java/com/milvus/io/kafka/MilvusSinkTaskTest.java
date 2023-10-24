package com.milvus.io.kafka;

import com.milvus.io.kafka.helper.MilvusClientHelper;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.*;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.GetLoadStateParam;
import io.milvus.param.dml.InsertParam;
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
    private MilvusServiceClient milvusClient;

    private Map<String, String> props;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        props = new HashMap<>();
        props.put(URL, "https://in01-aaaaaaaaa.aws-us-west-2.vectordb.zillizcloud.com:19541");
        props.put(TOKEN, "token_test");
        props.put(COLLECTION_NAME, "collection_test");
        // Add other required properties

        CollectionSchema collectionSchema = CollectionSchema.newBuilder()
                .addFields(FieldSchema.newBuilder().setName("id").setDataType(DataType.Int64).build())
                .addFields(FieldSchema.newBuilder().setName("name").setDataType(DataType.VarChar).build())
                .addFields(FieldSchema.newBuilder().setName("description").setDataType(DataType.VarChar).build())
                .addFields(FieldSchema.newBuilder().setName("description_vector").setDataType(DataType.FloatVector).build())
                .addFields(FieldSchema.newBuilder().setName("price").setDataType(DataType.Float).build())
                .build();

        DescribeCollectionResponse mockDescribeResponse = DescribeCollectionResponse.newBuilder()
                .setSchema(collectionSchema)
                .build();
        R<DescribeCollectionResponse> mockDescribeR = R.success(mockDescribeResponse);

        GetLoadStateResponse mockLoadStateResponse = GetLoadStateResponse.newBuilder()
                .setState(LoadState.LoadStateLoaded)
                .build();
        R<GetLoadStateResponse> mockLoadStateR = R.success(mockLoadStateResponse);

        when(milvusClient.describeCollection(any(DescribeCollectionParam.class))).thenReturn(mockDescribeR);
        when(milvusClient.getLoadState(any(GetLoadStateParam.class))).thenReturn(mockLoadStateR);

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
        verify(milvusClient).describeCollection(any(DescribeCollectionParam.class));
        verify(milvusClient).getLoadState(any(GetLoadStateParam.class));
        verify(milvusClient).insert(any(InsertParam.class));
    }

    public List<Float> generateVector(Integer dimensions){
        List<Float> floatList = new ArrayList<>();
        for (int k = 0; k < dimensions; ++k) {
            floatList.add(new Random().nextFloat());
        }
        return floatList;
    }
}
