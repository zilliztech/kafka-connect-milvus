package com.milvus.io.kafka;

import com.google.gson.JsonObject;
import static com.milvus.io.kafka.MilvusSinkConnectorConfig.TOKEN;
import com.milvus.io.kafka.client.MilvusRestClient;
import com.milvus.io.kafka.client.request.UpsertReq;
import com.milvus.io.kafka.client.response.DescribeCollectionResp;
import com.milvus.io.kafka.client.response.GetLoadStateResp;
import com.milvus.io.kafka.helper.MilvusClientHelper;
import com.milvus.io.kafka.utils.DataConverter;
import com.milvus.io.kafka.utils.Utils;
import com.milvus.io.kafka.utils.VersionUtil;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MilvusSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(MilvusSinkTask.class);
    private MilvusSinkConnectorConfig config;
    private MilvusRestClient myMilvusClient;
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
    protected void start(Map<String, String> props, MilvusRestClient milvusClient) {
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
        if (!myMilvusClient.hasCollection(config.getCollectionName())) {
            log.error("Collection not exist");
            throw new RuntimeException("Collection not exist" + config.getCollectionName());
        }
        // check if the collection is loaded
        GetLoadStateResp getLoadStateResp = myMilvusClient.getLoadState(config.getCollectionName());
        if (!Objects.equals(getLoadStateResp.getLoadState(), "LoadStateLoaded")) {
            log.error("Collection not loaded");
            throw new RuntimeException("Collection not loaded" + config.getCollectionName());
        }
        this.response = myMilvusClient.describeCollection(config.getCollectionName());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("Putting {} records to Milvus.", records.size());
        if (records.isEmpty()) {
            log.info("No records to put.");
            return;
        }

        // not support dynamic schema for now, for dynamic schema, we need to put the data into a JSONObject
        List<JsonObject> datas = new ArrayList<>();
        for (SinkRecord record : records) {
            log.debug("Writing {} to Milvus.", record);
            if (record.value() == null) {
                log.warn("Skipping record with null value.");
                continue;
            }
            try {
                JsonObject data = converter.convertRecord(record, response);
                datas.add(data);
            } catch (Exception e) {
                log.error("Failed to convert record to JSONObject, skip it", e);
            }
        }

        if (!response.getAutoID()) {
            // default to use upsert
            UpsertReq upsertReq = UpsertReq.builder()
                    .collectionName(config.getCollectionName())
                    .data(datas)
                    .build();
            log.info("Upserting data to collection: {} with datas: {}", config.getCollectionName(), datas);
            myMilvusClient.upsert(upsertReq);
        }

    }

    @Override
    public void stop() {
        log.info("Stopping Milvus client.");
    }
}
