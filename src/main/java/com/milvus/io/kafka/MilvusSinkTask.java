package com.milvus.io.kafka;

import com.alibaba.fastjson.JSONObject;
import com.milvus.io.kafka.helper.MilvusClientHelper;
import com.milvus.io.kafka.utils.DataConverter;
import com.milvus.io.kafka.utils.Utils;
import com.milvus.io.kafka.utils.VersionUtil;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.milvus.io.kafka.MilvusSinkConnectorConfig.TOKEN;

public class MilvusSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(MilvusSinkTask.class);
    private MilvusSinkConnectorConfig config;
    private MilvusClientV2 myMilvusClient;
    private DataConverter converter;
    private DescribeCollectionResp response;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        start(props, null);
    }

    // make visible for test
    protected void start(Map<String, String> props, MilvusClientV2 milvusClient) {
        log.info("Starting MilvusSinkTask.");
        props.put(TOKEN, Utils.encryptToken(props.get(TOKEN)));
        this.config = new MilvusSinkConnectorConfig(props);
        this.converter = new DataConverter(config);
        this.myMilvusClient = milvusClient == null ? new MilvusClientHelper().createMilvusClient(config) : milvusClient;
        log.info("Started MilvusSinkTask, Connecting to Zilliz Cluster:" + config.getUrl());
        preValidate();
    }

    private void preValidate() {
        // check if the collection exists
        if (!myMilvusClient.hasCollection(HasCollectionReq.builder().collectionName(config.getCollectionName()).build())) {
            log.error("Collection not exist");
            throw new RuntimeException("Collection not exist" + config.getCollectionName());
        }
        // check if the collection is loaded
        if (!myMilvusClient.getLoadState(GetLoadStateReq.builder().collectionName(config.getCollectionName()).build())){
            log.error("Collection not loaded");
            throw new RuntimeException("Collection not loaded" + config.getCollectionName());
        }
        this.response = myMilvusClient.describeCollection(DescribeCollectionReq.builder().collectionName(config.getCollectionName()).build());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("Putting {} records to Milvus.", records.size());
        if(records.isEmpty()) {
            log.info("No records to put.");
            return;
        }

        // not support dynamic schema for now, for dynamic schema, we need to put the data into a JSONObject
        List<JSONObject> datas = new ArrayList<>();
        for (SinkRecord record : records) {
            log.debug("Writing {} to Milvus.", record);
            if(record.value() == null) {
                log.warn("Skipping record with null value.");
                continue;
            }
            try {
                JSONObject data = converter.convertRecord(record, response.getCollectionSchema());
                datas.add(data);
            }catch (Exception e){
                log.error("Failed to convert record to JSONObject, skip it", e);
            }
        }

        if(!response.getAutoID()){
            // default to use upsert
            UpsertReq upsertReq = UpsertReq.builder()
                    .collectionName(config.getCollectionName())
                    .data(datas)
                    .build();
            log.info("Upserting data to collection: {} with datas: {}", config.getCollectionName(), datas);
            myMilvusClient.upsert(upsertReq);
        }else {
            InsertReq insertReq = InsertReq.builder()
                    .collectionName(config.getCollectionName())
                    .data(datas)
                    .build();
            log.info("Inserting data to collection: {} with fields: {}", config.getCollectionName(), datas.get(0).keySet());
            myMilvusClient.insert(insertReq);
        }

    }

    @Override
    public void stop() {
        log.info("Stopping Milvus client.");
        try {
            myMilvusClient.close(3);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
