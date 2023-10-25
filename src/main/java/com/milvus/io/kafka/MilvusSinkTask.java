package com.milvus.io.kafka;

import com.milvus.io.kafka.helper.MilvusClientHelper;
import com.milvus.io.kafka.utils.DataConverter;
import com.milvus.io.kafka.utils.Utils;
import com.milvus.io.kafka.utils.VersionUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.GetLoadStateResponse;
import io.milvus.grpc.LoadState;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.GetLoadStateParam;
import io.milvus.param.dml.InsertParam;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.milvus.io.kafka.MilvusSinkConnectorConfig.TOKEN;

public class MilvusSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(MilvusSinkTask.class);
    private MilvusSinkConnectorConfig config;
    private MilvusServiceClient myMilvusClient;
    private DataConverter converter;
    private CollectionSchema collectionSchema;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        start(props, null);
    }

    // make visible for test
    protected void start(Map<String, String> props, MilvusServiceClient milvusClient) {
        log.info("Starting MilvusSinkTask.");
        props.put(TOKEN, Utils.encryptToken(props.get(TOKEN)));
        this.config = new MilvusSinkConnectorConfig(props);
        this.converter = new DataConverter(config);
        this.myMilvusClient = milvusClient == null ? new MilvusClientHelper().createMilvusClient(config) : milvusClient;
        this.collectionSchema = GetCollectionInfo(config.getCollectionName());

        log.info("Started MilvusSinkTask, Connecting to Zilliz Cluster:" + config.getUrl());

    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("Putting {} records to Milvus.", records.size());

        for (SinkRecord record : records) {
            log.debug("Writing {} to Milvus.", record);
            WriteRecord(record, collectionSchema);
        }
    }

    protected CollectionSchema GetCollectionInfo(String collectionName) {
        // check if the collection exists
        R<DescribeCollectionResponse> response = myMilvusClient.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(collectionName).build());
        if (response.getData() == null) {
            log.error("Collection not exist");
            throw new RuntimeException("Collection not exist" + collectionName);
        }else {
            GetLoadStateParam getLoadStateParam = GetLoadStateParam.newBuilder()
                    .withCollectionName(collectionName)
                    .build();
            R<GetLoadStateResponse> loadState = myMilvusClient.getLoadState(getLoadStateParam);
            if (loadState.getData().getState() != LoadState.LoadStateLoaded){
                log.error("Collection not loaded");
                throw new RuntimeException("Collection not loaded" + collectionName);
            }
        }
        return response.getData().getSchema();
    }

    protected void WriteRecord(SinkRecord record, CollectionSchema collectionSchema) {
        // not support dynamic schema for now, for dynamic schema, we need to put the data into a JSONObject
        List<InsertParam.Field> fields = converter.convertRecord(record, collectionSchema);
        InsertParam insertParam = InsertParam.newBuilder()
                    .withCollectionName(config.getCollectionName())
                    .withFields(fields)
                    .build();

        log.info("Inserting data to collection: " + config.getCollectionName() + " with fields: " +
                insertParam.getFields());
        myMilvusClient.insert(insertParam);
    }

    @Override
    public void stop() {
        log.info("Stopping Milvus client.");
        myMilvusClient.close();
    }
}
